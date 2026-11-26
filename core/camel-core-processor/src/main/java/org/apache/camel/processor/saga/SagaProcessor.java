/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.processor.saga;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.concurrent.CompletableFuture;

import org.apache.camel.AsyncCallback;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.Traceable;
import org.apache.camel.saga.CamelSagaCoordinator;
import org.apache.camel.saga.CamelSagaService;
import org.apache.camel.saga.CamelSagaStep;
import org.apache.camel.spi.IdAware;
import org.apache.camel.spi.RouteIdAware;
import org.apache.camel.support.processor.DelegateAsyncProcessor;
import org.apache.camel.support.service.ServiceHelper;
import org.apache.camel.util.ObjectHelper;

/**
 * Processor for handling sagas.
 */
public abstract class SagaProcessor extends DelegateAsyncProcessor implements Traceable, IdAware, RouteIdAware {

    /**
     * Exchange property holding the id of the saga the exchange takes part in. Unlike the {@code Long-Running-Action}
     * header it is not populated from incoming messages, so it acts as the exchange's internal saga state.
     */
    private static final String SAGA_LONG_RUNNING_ACTION_PROPERTY = "CamelSagaLongRunningAction";

    /**
     * Saga service taking part in the LRA protocol, which carries the coordinator id in the
     * {@code Long-Running-Action} header (camel-lra is not a dependency of this module, so it is matched by name).
     */
    private static final String LRA_SAGA_SERVICE_CLASS = "org.apache.camel.service.lra.LRASagaService";

    /**
     * Name of the method a saga service declares to advertise that the {@code Long-Running-Action} header may select a
     * coordinator.
     */
    private static final String HEADER_SUPPORTED_METHOD = "isLongRunningActionHeaderSupported";

    protected final CamelSagaService sagaService;

    protected final CamelSagaStep step;

    protected final SagaCompletionMode completionMode;

    private final boolean lraSagaService;
    private final Method headerSupportedMethod;

    private String id;
    private String routeId;

    public SagaProcessor(CamelContext camelContext, Processor childProcessor, CamelSagaService sagaService,
                         SagaCompletionMode completionMode, CamelSagaStep step) {
        super(ObjectHelper.notNull(childProcessor, "childProcessor"));
        this.sagaService = ObjectHelper.notNull(sagaService, "sagaService");
        this.completionMode = ObjectHelper.notNull(completionMode, "completionMode");
        this.step = ObjectHelper.notNull(step, "step");
        this.lraSagaService = isLraSagaService(sagaService.getClass());
        this.headerSupportedMethod = findHeaderSupportedMethod(sagaService.getClass());
    }

    protected CompletableFuture<CamelSagaCoordinator> getCurrentSagaCoordinator(Exchange exchange) {
        // try internal state first
        String currentSaga = exchange.getProperty(SAGA_LONG_RUNNING_ACTION_PROPERTY, String.class);
        if (currentSaga == null && isLongRunningActionHeaderSupported()) {
            // fall back to header for interoperability (e.g., LRA protocol), but only for a service that takes part
            // in such a protocol. Long-Running-Action is outside the Camel namespace that consumers filter, and the
            // id is written back onto responses, so consulting it where no external coordinator exists would let a
            // message pick which saga its exchange joins.
            currentSaga = exchange.getIn().getHeader(Exchange.SAGA_LONG_RUNNING_ACTION, String.class);
        }
        if (currentSaga != null) {
            return sagaService.getSaga(currentSaga);
        }

        return CompletableFuture.completedFuture(null);
    }

    protected void setCurrentSagaCoordinator(Exchange exchange, CamelSagaCoordinator coordinator) {
        if (coordinator != null) {
            exchange.setProperty(SAGA_LONG_RUNNING_ACTION_PROPERTY, coordinator.getId());
            exchange.getIn().setHeader(Exchange.SAGA_LONG_RUNNING_ACTION, coordinator.getId());
        } else {
            exchange.removeProperty(SAGA_LONG_RUNNING_ACTION_PROPERTY);
            exchange.getIn().removeHeader(Exchange.SAGA_LONG_RUNNING_ACTION);
            exchange.getMessage().removeHeader(Exchange.SAGA_LONG_RUNNING_ACTION);
        }
    }

    /**
     * Whether a saga coordinator may be selected from the {@code Long-Running-Action} message header.
     * <p>
     * That only makes sense for a saga service which actually participates in a distributed saga protocol, such as the
     * LRA one. A service advertises it by declaring a public {@code boolean isLongRunningActionHeaderSupported()}
     * method returning true; the LRA service supports it, and any other service (such as the in-memory one) does not.
     */
    private boolean isLongRunningActionHeaderSupported() {
        if (headerSupportedMethod != null) {
            try {
                return Boolean.TRUE.equals(headerSupportedMethod.invoke(sagaService));
            } catch (Exception e) {
                return false;
            }
        }
        return lraSagaService;
    }

    private static boolean isLraSagaService(Class<?> type) {
        for (Class<?> current = type; current != null; current = current.getSuperclass()) {
            if (LRA_SAGA_SERVICE_CLASS.equals(current.getName())) {
                return true;
            }
        }
        return false;
    }

    private static Method findHeaderSupportedMethod(Class<?> type) {
        try {
            Method method = type.getMethod(HEADER_SUPPORTED_METHOD);
            if (method.getReturnType() != boolean.class || Modifier.isStatic(method.getModifiers())) {
                return null;
            }
            // the declaring class may not be public (such as a nested class)
            method.trySetAccessible();
            return method;
        } catch (Exception e) {
            return null;
        }
    }

    protected void handleSagaCompletion(
            Exchange exchange, CamelSagaCoordinator coordinator, CamelSagaCoordinator previousCoordinator,
            AsyncCallback callback) {
        if (this.completionMode == SagaCompletionMode.AUTO) {
            if (exchange.getException() != null) {
                if (coordinator != null) {
                    coordinator.compensate(exchange).whenComplete((done, ex) -> ifNotException(ex, exchange, callback, () -> {
                        setCurrentSagaCoordinator(exchange, previousCoordinator);
                        callback.done(false);
                    }));
                } else {
                    // No coordinator available, so no saga available.
                    callback.done(false);
                }
            } else {
                coordinator.complete(exchange).whenComplete((done, ex) -> ifNotException(ex, exchange, callback, () -> {
                    setCurrentSagaCoordinator(exchange, previousCoordinator);
                    callback.done(false);
                }));
            }
        } else if (this.completionMode == SagaCompletionMode.MANUAL) {
            // Completion will be handled manually by the user
            callback.done(false);
        } else {
            throw new IllegalStateException("Unsupported completion mode: " + this.completionMode);
        }
    }

    public CamelSagaService getSagaService() {
        return sagaService;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String getRouteId() {
        return routeId;
    }

    @Override
    public void setRouteId(String routeId) {
        this.routeId = routeId;
    }

    @Override
    public String toString() {
        return "id";
    }

    @Override
    public String getTraceLabel() {
        return "saga";
    }

    protected void ifNotException(Throwable ex, Exchange exchange, AsyncCallback callback, Runnable code) {
        ifNotException(ex, exchange, false, null, null, callback, code);
    }

    protected void ifNotException(
            Throwable ex, Exchange exchange, boolean handleCompletion, CamelSagaCoordinator coordinator,
            CamelSagaCoordinator previousCoordinator, AsyncCallback callback, Runnable code) {
        if (ex != null) {
            exchange.setException(ex);
            if (handleCompletion) {
                handleSagaCompletion(exchange, coordinator, previousCoordinator, callback);
            } else {
                callback.done(false);
            }
        } else {
            code.run();
        }
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        ServiceHelper.startService(sagaService);
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        ServiceHelper.stopService(sagaService);
    }
}
