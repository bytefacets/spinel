// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.send;

import static com.bytefacets.spinel.comms.send.DefaultSubscriptionProvider.defaultSubscriptionProvider;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.bytefacets.spinel.TransformInput;
import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.comms.send.ConnectedSessionInfo;
import com.bytefacets.spinel.comms.send.DefaultSubscriptionProvider;
import com.bytefacets.spinel.comms.send.OutputRegistry;
import com.bytefacets.spinel.grpc.proto.CreateSubscription;
import com.bytefacets.spinel.grpc.proto.InitializationRequest;
import com.bytefacets.spinel.grpc.proto.RequestType;
import com.bytefacets.spinel.grpc.proto.Response;
import com.bytefacets.spinel.grpc.proto.ResponseType;
import com.bytefacets.spinel.grpc.proto.SubscriptionRequest;
import com.bytefacets.spinel.grpc.proto.SubscriptionResponse;
import com.bytefacets.spinel.grpc.send.auth.GrpcConnectedSessionInfo;
import io.grpc.stub.ServerCallStreamObserver;
import io.netty.channel.EventLoop;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GrpcSessionTest {
    private static final ConnectedSessionInfo sessionInfo = GrpcConnectedSessionInfo.EMPTY;
    private @Mock EventLoop dataExecutor;
    private @Mock OutputRegistry registry;
    private @Mock Consumer<GrpcSession> onComplete;
    private @Mock ServerCallStreamObserver<SubscriptionResponse> observer;
    private @Mock TransformOutput output;
    private @Captor ArgumentCaptor<SubscriptionResponse> responseCaptor;
    private @Captor ArgumentCaptor<Runnable> runnableCaptor;
    private GrpcSession session;

    @BeforeEach
    void setUp() {
        final DefaultSubscriptionProvider subscriptionProvider =
                defaultSubscriptionProvider(registry);
        session =
                new GrpcSession(
                        sessionInfo, subscriptionProvider, observer, dataExecutor, onComplete);
        lenient().when(registry.lookup("foo")).thenReturn(output);
        lenient()
                .doAnswer(
                        inv -> {
                            final var input = inv.getArgument(0, TransformInput.class);
                            input.setSource(output);
                            return null;
                        })
                .when(output)
                .attachInput(any(TransformInput.class));
    }

    @Test
    void shouldCloseOnDataThread() {
        // given subscription is set up
        session.requestHandler().onNext(subscribeRequest("foo", List.of()));
        reset(output);
        assertThat(session.activeAdapters(), equalTo(1));
        // when
        session.close();
        assertThat(session.activeAdapters(), equalTo(1));
        // then
        verifyNoInteractions(output);
        // when running the data thread
        verify(dataExecutor, times(1)).execute(runnableCaptor.capture());
        runnableCaptor.getValue().run();
        verify(output, times(1)).detachInput(any(TransformInput.class));
        assertThat(session.activeAdapters(), equalTo(0));
    }

    @Test
    void shouldReturnErrorMessageWhenUnknownRequest() {
        session.requestHandler()
                .onNext(
                        SubscriptionRequest.newBuilder()
                                .setMsgToken(123)
                                .setSubscriptionId(77)
                                .setRequestTypeValue(5)
                                .build());
        // then
        verify(observer, times(1)).onNext(responseCaptor.capture());
        assertThat(
                responseCaptor.getValue().getResponseType(),
                equalTo(ResponseType.RESPONSE_TYPE_MESSAGE));
        assertThat(responseCaptor.getValue().getResponse().getSuccess(), equalTo(false));
        assertThat(
                responseCaptor.getValue().getResponse().getMessage(),
                equalTo("Request type not understood: 5"));
    }

    @Nested
    class CreateSubscriptionTests {
        @Test
        void shouldConnectSourceToSessionOnSubscribe() {
            session.requestHandler().onNext(subscribeRequest("foo", List.of()));

            verify(registry, times(1)).lookup("foo");
            verify(output, times(1)).attachInput(any(TransformInput.class));
            assertThat(session.activeAdapters(), equalTo(1));
        }

        @Test
        void shouldReturnErrorMessageWhenOutputNotFound() {
            // when
            session.requestHandler().onNext(subscribeRequest("bar", List.of()));
            // then
            verify(observer, times(1)).onNext(responseCaptor.capture());
            assertThat(
                    responseCaptor.getValue().getResponseType(),
                    equalTo(ResponseType.RESPONSE_TYPE_MESSAGE));
            assertThat(responseCaptor.getValue().getResponse().getSuccess(), equalTo(false));
            assertThat(
                    responseCaptor.getValue().getResponse().getMessage(),
                    equalTo("Output not found: bar"));
        }
    }

    @Nested
    class InitializationTests {
        @Test
        void shouldRespondToInitializationRequest() {
            session.requestHandler().onNext(init(1, "anonymous"));
            verify(observer, times(1)).onNext(responseCaptor.capture());
            final var expected =
                    SubscriptionResponse.newBuilder()
                            .setRefMsgToken(1)
                            .setResponseType(ResponseType.RESPONSE_TYPE_INIT)
                            .setResponse(
                                    Response.newBuilder()
                                            .setMessage("hello")
                                            .setSuccess(true)
                                            .build())
                            .build();
            assertThat(responseCaptor.getValue(), equalTo(expected));
        }
    }

    SubscriptionRequest init(final int token, final String msg) {
        final var init = InitializationRequest.newBuilder().setMessage(msg).build();
        return SubscriptionRequest.newBuilder()
                .setRequestType(RequestType.REQUEST_TYPE_INIT)
                .setMsgToken(token)
                .setInitialization(init)
                .build();
    }

    SubscriptionRequest subscribeRequest(final String name, final List<String> fields) {
        final var sub = CreateSubscription.newBuilder().setName(name).addAllFieldNames(fields);
        return SubscriptionRequest.newBuilder()
                .setMsgToken(123)
                .setSubscriptionId(77)
                .setRequestType(RequestType.REQUEST_TYPE_SUBSCRIBE)
                .setSubscription(sub)
                .build();
    }
}
