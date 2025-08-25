// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.receive;

import static com.bytefacets.spinel.comms.SubscriptionConfig.subscriptionConfig;
import static com.bytefacets.spinel.comms.subscription.ModificationRequestFactory.applyFilterExpression;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.bytefacets.spinel.comms.receive.SubscriptionListener;
import com.bytefacets.spinel.comms.send.ModificationResponse;
import com.bytefacets.spinel.comms.subscription.ModificationRequest;
import com.bytefacets.spinel.comms.subscription.ModificationRequestFactory;
import com.bytefacets.spinel.grpc.codec.ObjectDecoderRegistry;
import com.bytefacets.spinel.grpc.proto.ModificationAddRemove;
import com.bytefacets.spinel.grpc.proto.RequestType;
import com.bytefacets.spinel.grpc.proto.Response;
import com.bytefacets.spinel.grpc.proto.ResponseType;
import com.bytefacets.spinel.grpc.proto.SubscriptionRequest;
import com.bytefacets.spinel.grpc.proto.SubscriptionResponse;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SubscriptionTest {
    private @Mock GrpcDecoder decoder;
    private @Mock GrpcClient.MessageSink msgSink;
    private @Mock SubscriptionListener listener;
    private @Captor ArgumentCaptor<SubscriptionRequest> requestCaptor;
    private final ModificationRequest request = applyFilterExpression("a == 1");
    private Subscription sub;

    @BeforeEach
    void setUp() {
        lenient().when(msgSink.isConnected()).thenReturn(true);
        final var config = subscriptionConfig("x").build();
        sub = new Subscription(3, decoder, config, new MsgHelp(), msgSink, listener);
    }

    @Test
    void shouldAddActiveModificationsToCreateSubscriptionRequest() {
        // given
        when(msgSink.isConnected()).thenReturn(false);
        sub.add(request);
        sub.add(applyFilterExpression("a == 2"));
        lenient().when(msgSink.isConnected()).thenReturn(true);
        // when
        sub.requestSubscriptionIfNecessary();
        //
        verify(msgSink, times(1)).accept(requestCaptor.capture());
        final var msg = requestCaptor.getValue();
        final var create = msg.getSubscription();
        assertThat(create.getModificationsCount(), equalTo(2));
        final var observed =
                create.getModificationsList().stream()
                        .map(
                                mod -> {
                                    final var args =
                                            mod.getArgumentsList().stream()
                                                    .map(ObjectDecoderRegistry::decode)
                                                    .toArray();
                                    return ModificationRequestFactory.request(
                                            mod.getTarget(), mod.getAction(), args);
                                })
                        .collect(Collectors.toSet());
        assertThat(observed, containsInAnyOrder(request, applyFilterExpression("a == 2")));
    }

    @Nested
    class AddModificationTests {
        @Test
        void shouldSendAddModification() {
            sub.add(request);
            verify(msgSink, times(1)).accept(requestCaptor.capture());
            final var msg = requestCaptor.getValue();
            assertThat(msg.getModification().getAddRemove(), equalTo(ModificationAddRemove.ADD));
            validateRequest(msg);
        }

        @Test
        void shouldCallBackOriginalListenerWhenDone() {
            sub.add(request);
            verify(msgSink, times(1)).accept(requestCaptor.capture());
            final var msg = requestCaptor.getValue();
            sub.accept(response(msg.getMsgToken(), false, "hey!!!"));
            verify(listener, times(1))
                    .onModificationAddResponse(
                            request, new ModificationResponse(true, "hey!!!", null));
        }

        @SuppressWarnings("unchecked")
        @Test
        void shouldOnlySendWhenRefCountEqualsOne() {
            sub.add(request); // reference count = 1
            reset(msgSink);
            sub.add(request); // reference count = 2
            verifyNoInteractions(msgSink);
            verifyNoInteractions(listener);
        }
    }

    @Nested
    class RemoveModificationTests {
        @SuppressWarnings("unchecked")
        @BeforeEach
        void setUp() {
            sub.add(request);
            reset(msgSink);
        }

        @Test
        void shouldSendRemoveModification() {
            sub.remove(request);
            verify(msgSink, times(1)).accept(requestCaptor.capture());
            final var msg = requestCaptor.getValue();
            assertThat(msg.getModification().getAddRemove(), equalTo(ModificationAddRemove.REMOVE));
            validateRequest(msg);
        }

        @Test
        void shouldCallBackOriginalListenerWhenDone() {
            sub.remove(request);
            verify(msgSink, times(1)).accept(requestCaptor.capture());
            final var msg = requestCaptor.getValue();
            sub.accept(response(msg.getMsgToken(), false, "hey!!!"));
            verify(listener, times(1))
                    .onModificationRemoveResponse(
                            request, new ModificationResponse(true, "hey!!!", null));
        }

        @Test
        void shouldNotSendRemoveWhenRefCountGTZero() {
            sub.add(request); // reference count = 2
            sub.remove(request); // reference count = 1
            verifyNoInteractions(msgSink);
            verifyNoInteractions(listener);
        }
    }

    private void validateRequest(final SubscriptionRequest msg) {
        assertThat(msg.getRequestType(), equalTo(RequestType.REQUEST_TYPE_MODIFY));
        assertThat(msg.getModification().getAction(), equalTo(request.action()));
        assertThat(msg.getModification().getTarget(), equalTo(request.target()));
        assertThat(msg.getModification().getArgumentsCount(), equalTo(1));
        final var expression = ObjectDecoderRegistry.decode(msg.getModification().getArguments(0));
        assertThat(expression, equalTo("a == 1"));
    }

    private SubscriptionResponse response(
            final int token, final boolean error, final String message) {
        return SubscriptionResponse.newBuilder()
                .setRefMsgToken(token)
                .setResponseType(ResponseType.RESPONSE_TYPE_MESSAGE)
                .setResponse(Response.newBuilder().setSuccess(!error).setMessage(message).build())
                .build();
    }
}
