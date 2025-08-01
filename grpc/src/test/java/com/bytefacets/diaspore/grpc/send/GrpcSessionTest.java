package com.bytefacets.diaspore.grpc.send;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.bytefacets.diaspore.TransformInput;
import com.bytefacets.diaspore.TransformOutput;
import com.bytefacets.diaspore.comms.send.OutputRegistry;
import com.bytefacets.diaspore.grpc.proto.CreateSubscription;
import com.bytefacets.diaspore.grpc.proto.InitializationRequest;
import com.bytefacets.diaspore.grpc.proto.RequestType;
import com.bytefacets.diaspore.grpc.proto.Response;
import com.bytefacets.diaspore.grpc.proto.ResponseType;
import com.bytefacets.diaspore.grpc.proto.SubscriptionRequest;
import com.bytefacets.diaspore.grpc.proto.SubscriptionResponse;
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
        session = new GrpcSession(registry, observer, dataExecutor, onComplete);
        lenient().when(registry.lookup("foo")).thenReturn(output);
        lenient()
                .doAnswer(
                        inv -> {
                            final var input = inv.getArgument(0, TransformInput.class);
                            input.setSource(output);
                            return null;
                        })
                .when(output)
                .attachInput(any());
    }

    @Test
    void shouldCloseOnDataThread() {
        // given subscription is set up
        session.requestHandler().onNext(subscribeRequest("foo", List.of()));
        verify(dataExecutor, times(1)).execute(runnableCaptor.capture());
        runnableCaptor.getValue().run();
        reset(dataExecutor, output);
        assertThat(session.activeAdapters(), equalTo(1));
        // when
        session.close();
        assertThat(session.activeAdapters(), equalTo(1));
        // then
        verifyNoInteractions(output);
        // when running the data thread
        verify(dataExecutor, times(1)).execute(runnableCaptor.capture());
        runnableCaptor.getValue().run();
        verify(output, times(1)).detachInput(any());
        assertThat(session.activeAdapters(), equalTo(0));
    }

    @Test
    void shouldReturnErrorMessageWhenUnknownRequest() {
        session.requestHandler()
                .onNext(
                        SubscriptionRequest.newBuilder()
                                .setRefToken(123)
                                .setRequestTypeValue(5)
                                .build());
        // then
        verify(observer, times(1)).onNext(responseCaptor.capture());
        assertThat(
                responseCaptor.getValue().getResponseType(),
                equalTo(ResponseType.RESPONSE_TYPE_MESSAGE));
        assertThat(responseCaptor.getValue().getResponse().getError(), equalTo(true));
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
            verify(output, times(1)).attachInput(any());
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
            assertThat(responseCaptor.getValue().getResponse().getError(), equalTo(true));
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
                            .setRefToken(1)
                            .setResponseType(ResponseType.RESPONSE_TYPE_INIT)
                            .setResponse(Response.newBuilder().setMessage("hello").build())
                            .build();
            assertThat(responseCaptor.getValue(), equalTo(expected));
        }
    }

    SubscriptionRequest init(final int token, final String name) {
        final var init = InitializationRequest.newBuilder().setUser(name).build();
        return SubscriptionRequest.newBuilder()
                .setRequestType(RequestType.REQUEST_TYPE_INIT)
                .setRefToken(token)
                .setInitialization(init)
                .build();
    }

    SubscriptionRequest subscribeRequest(final String name, final List<String> fields) {
        final var sub = CreateSubscription.newBuilder().setName(name).addAllFieldNames(fields);
        return SubscriptionRequest.newBuilder()
                .setRefToken(123)
                .setRequestType(RequestType.REQUEST_TYPE_SUBSCRIBE)
                .setSubscription(sub)
                .build();
    }
}
