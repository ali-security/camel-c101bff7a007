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
package org.apache.camel.processor;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.camel.ContextTestSupport;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.SagaCompletionMode;
import org.apache.camel.model.SagaPropagation;
import org.apache.camel.saga.CamelSagaCoordinator;
import org.apache.camel.saga.CamelSagaStep;
import org.apache.camel.saga.InMemorySagaService;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The saga id normally travels in the exchange's internal state. It is also readable from the
 * {@code Long-Running-Action} header so a coordinator started elsewhere can be joined, which is how the LRA protocol
 * carries it - but that header sits outside the {@code Camel} namespace consumers filter, and the id is written back
 * onto responses, so under a service with no external coordinator it would let a message choose which saga its exchange
 * joins.
 * <p>
 * Asserted by watching which ids reach {@code getSaga}. Sagas named by the header are started with manual completion,
 * so they are still running when a second exchange presents their id.
 */
public class SagaHeaderCannotSelectCoordinatorTest extends ContextTestSupport {

    private final RecordingSagaService sagaService = new RecordingSagaService();

    private final RecordingSagaService headerSupportingSagaService = new HeaderSupportingSagaService();

    @Test
    public void aMessageSuppliedIdIsNotLookedUp() {
        Exchange exchange = template.request("direct:mandatory", e -> {
            e.getIn().setHeader(Exchange.SAGA_LONG_RUNNING_ACTION, "a-saga-id-from-the-wire");
            e.getIn().setBody("hello");
        });

        assertTrue(exchange.isFailed(), "MANDATORY has no saga to join, so the exchange must fail");
        assertEquals(List.of(), sagaService.lookedUp,
                "the coordinator must not be looked up from an id supplied by the message");
    }

    @Test
    public void aRunningSagaCannotBeJoinedThroughTheHeader() {
        String runningSaga = startRunningSaga("direct:start");

        Exchange exchange = template.request("direct:mandatory", e -> {
            e.getIn().setHeader(Exchange.SAGA_LONG_RUNNING_ACTION, runningSaga);
            e.getIn().setBody("hello");
        });

        assertTrue(exchange.isFailed(), "the exchange must not join the running saga named by its header");
        assertEquals(List.of(), sagaService.lookedUp,
                "the coordinator must not be looked up from an id supplied by the message");
    }

    @Test
    public void aRequiredSagaDoesNotInheritTheSagaNamedByTheHeader() {
        String runningSaga = startRunningSaga("direct:start");

        Exchange exchange = template.request("direct:required", e -> {
            e.getIn().setHeader(Exchange.SAGA_LONG_RUNNING_ACTION, runningSaga);
            e.getIn().setBody("hello");
        });

        assertFalse(exchange.isFailed());
        String joinedSaga = exchange.getMessage().getHeader("joinedSaga", String.class);
        assertNotNull(joinedSaga);
        assertNotEquals(runningSaga, joinedSaga,
                "REQUIRED must start its own saga rather than join the one in the header");
        assertEquals(List.of(), sagaService.lookedUp,
                "the coordinator must not be looked up from an id supplied by the message");
    }

    @Test
    public void aHeaderChangedWithinASagaDoesNotMoveTheExchangeToAnotherSaga() {
        String runningSaga = startRunningSaga("direct:start");

        Exchange exchange = template.request("direct:outer", e -> {
            e.getIn().setHeader("otherSaga", runningSaga);
            e.getIn().setBody("hello");
        });

        assertFalse(exchange.isFailed());
        String outerSaga = exchange.getMessage().getHeader("outerSaga", String.class);
        assertNotNull(outerSaga);
        assertNotEquals(runningSaga, outerSaga);
        assertEquals(List.of(outerSaga), sagaService.lookedUp,
                "the nested step must join the saga of the exchange, not the one named by the header");
    }

    @Test
    public void aServiceSupportingTheHeaderJoinsTheSagaItNames() {
        String runningSaga = startRunningSaga("direct:start-header-supported");

        Exchange exchange = template.request("direct:mandatory-header-supported", e -> {
            e.getIn().setHeader(Exchange.SAGA_LONG_RUNNING_ACTION, runningSaga);
            e.getIn().setBody("hello");
        });

        assertFalse(exchange.isFailed(),
                "a service taking part in a distributed saga protocol joins through the header");
        assertEquals(List.of(runningSaga), headerSupportingSagaService.lookedUp);
    }

    private String startRunningSaga(String uri) {
        Exchange started = template.request(uri, e -> e.getIn().setBody("start"));
        assertFalse(started.isFailed());
        // the id of the saga is written back onto the response
        String id = started.getMessage().getHeader(Exchange.SAGA_LONG_RUNNING_ACTION, String.class);
        assertNotNull(id);
        return id;
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                context.addService(sagaService);
                headerSupportingSagaService.setCamelContext(context);

                from("direct:mandatory").saga().propagation(SagaPropagation.MANDATORY)
                        .transform().constant("joined");

                from("direct:start").saga().completionMode(SagaCompletionMode.MANUAL)
                        .transform().constant("started");

                from("direct:required").saga().propagation(SagaPropagation.REQUIRED)
                        .setHeader("joinedSaga", header(Exchange.SAGA_LONG_RUNNING_ACTION));

                from("direct:outer").saga()
                        .setHeader("outerSaga", header(Exchange.SAGA_LONG_RUNNING_ACTION))
                        .setHeader(Exchange.SAGA_LONG_RUNNING_ACTION, header("otherSaga"))
                        .to("direct:mandatory");

                from("direct:start-header-supported").saga().sagaService(headerSupportingSagaService)
                        .completionMode(SagaCompletionMode.MANUAL)
                        .transform().constant("started");

                from("direct:mandatory-header-supported").saga().sagaService(headerSupportingSagaService)
                        .propagation(SagaPropagation.MANDATORY)
                        .transform().constant("joined");
            }
        };
    }

    /**
     * Delegates to the in-memory service, recording every id it is asked to resolve.
     */
    private static class RecordingSagaService extends InMemorySagaService {

        private final List<String> lookedUp = new CopyOnWriteArrayList<>();

        @Override
        public CompletableFuture<CamelSagaCoordinator> getSaga(String id) {
            lookedUp.add(id);
            return super.getSaga(id);
        }

        @Override
        public void registerStep(CamelSagaStep step) {
            super.registerStep(step);
        }
    }

    /**
     * A service taking part in a distributed saga protocol, which advertises that the {@code Long-Running-Action}
     * header may select a coordinator.
     */
    private static final class HeaderSupportingSagaService extends RecordingSagaService {

        public boolean isLongRunningActionHeaderSupported() {
            return true;
        }
    }
}
