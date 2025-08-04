package com.bytefacets.diaspore.grpc.receive;

import static com.bytefacets.diaspore.schema.MatrixStoreFieldFactory.matrixStoreFieldFactory;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.bytefacets.diaspore.comms.ConnectionInfo;
import com.bytefacets.diaspore.comms.SubscriptionConfig;
import com.bytefacets.diaspore.grpc.proto.DataServiceGrpc;
import com.bytefacets.diaspore.grpc.proto.Response;
import com.bytefacets.diaspore.grpc.proto.ResponseType;
import com.bytefacets.diaspore.grpc.proto.SubscriptionRequest;
import com.bytefacets.diaspore.grpc.proto.SubscriptionResponse;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.netty.channel.EventLoop;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GrpcClientTest {
    private final ConnectionInfo cxInfo = new ConnectionInfo("test", "direct://test");
    private final SchemaBuilder schemaBuilder =
            new SchemaBuilder(matrixStoreFieldFactory(16, 16, i -> {}));
    private final SubscriptionStore subscriptionStore = new SubscriptionStore(cxInfo);
    private @Mock ManagedChannel channel;
    private @Mock EventLoop dataEventLoop;
    private @Mock DataServiceGrpc.DataServiceStub serviceStub;
    private @Mock Function<SchemaBuilder, GrpcDecoder> decoderSupplier;
    private @Mock GrpcDecoder decoder;
    private @Mock StreamObserver<SubscriptionRequest> requestStream;
    private @Captor ArgumentCaptor<SubscriptionRequest> requestCaptor;
    private @Captor ArgumentCaptor<StreamObserver<SubscriptionResponse>> responseAdapterCaptor;
    private GrpcClient client;

    @BeforeEach
    void setUp() {
        client =
                new GrpcClient(
                        cxInfo,
                        channel,
                        dataEventLoop,
                        serviceStub,
                        decoderSupplier,
                        subscriptionStore);
        lenient().when(serviceStub.subscribe(any())).thenReturn(requestStream);
        lenient().when(decoderSupplier.apply(any())).thenReturn(decoder);
        lenient().when(channel.getState(false)).thenReturn(ConnectivityState.READY);
    }

    @Test
    void shouldCreateAndSendSubscriptionWhenInitializeCompleted() {
        client.markSessionInitialized(true);
        client.connection().connect();
        final var config = SubscriptionConfig.subscriptionConfig("foo").build();
        final var sub = client.createSubscription(schemaBuilder, config);
        assertThat(sub.isSubscribed(), equalTo(true));
        verify(requestStream, times(1)).onNext(eq(sub.createRequest()));
    }

    @Test
    void shouldCreateAndNotSendSubscriptionWhenNotConnected() {
        final var config = SubscriptionConfig.subscriptionConfig("foo").build();
        final var sub = client.createSubscription(schemaBuilder, config);
        assertThat(sub.isSubscribed(), equalTo(false));
        verifyNoInteractions(requestStream);
    }

    @Nested
    class ConnectTests {
        @Test
        void shouldSubscribeToServiceWhenConnecting() {
            client.connection().connect();
            verify(serviceStub, times(1)).subscribe(any());
        }

        @ParameterizedTest
        @EnumSource(ConnectivityState.class)
        void shouldReportConnectedWhenChannelIsReady(final ConnectivityState state) {
            when(channel.getState(false)).thenReturn(ConnectivityState.SHUTDOWN);
            client.connection().connect();
            assertThat(client.connection().isConnected(), equalTo(false));
            when(channel.getState(false)).thenReturn(state);
            assertThat(
                    client.connection().isConnected(),
                    equalTo(state.equals(ConnectivityState.READY)));
        }
    }

    @Nested
    class InitializationTests {
        @Test
        void shouldStartInitializationWhenConnectionReady() {
            client.connection().connect();
            assertThat(client.isSessionInitialized(), equalTo(false));
            assertThat(client.isInitializationInProgress(), equalTo(true));
            verify(requestStream, times(1)).onNext(requestCaptor.capture());
            assertThat(requestCaptor.getValue(), equalTo(MsgHelp.init(1, "hello")));
        }

        @Test
        void shouldScheduleLaterInitializationAttempt() {
            client.connection().connect();
            verify(dataEventLoop, times(1))
                    .schedule(any(Runnable.class), eq(1L), eq(TimeUnit.SECONDS));
        }

        @Test
        void shouldEndInitializationWhenResponseReceived() {
            client.connection().connect();
            verify(serviceStub, times(1)).subscribe(responseAdapterCaptor.capture());
            responseAdapterCaptor.getValue().onNext(initResponse(1));
            assertThat(client.isInitializationInProgress(), equalTo(false));
            assertThat(client.isSessionInitialized(), equalTo(true));
        }

        @Test
        void shouldNotSendInitializationIfStateIsNoLongerReady() {
            lenient()
                    .when(channel.getState(false))
                    .thenReturn(ConnectivityState.READY, ConnectivityState.TRANSIENT_FAILURE);
            client.connection().connect();
            verifyNoInteractions(requestStream, dataEventLoop);
        }

        private SubscriptionResponse initResponse(final int token) {
            final var response = Response.newBuilder().setError(false).setMessage("hello").build();
            return SubscriptionResponse.newBuilder()
                    .setRefToken(token)
                    .setResponseType(ResponseType.RESPONSE_TYPE_INIT)
                    .setResponse(response)
                    .build();
        }
    }

    @SuppressWarnings("unchecked")
    @Nested
    class DisconnectTests {
        @Test
        void shouldUnsubscribeWhenDisconnecting() {
            client.connection().connect();
            final var config = SubscriptionConfig.subscriptionConfig("foo").build();
            final var sub = client.createSubscription(schemaBuilder, config);
            reset(requestStream);
            // when
            client.connection().disconnect();
            verify(requestStream, times(1)).onCompleted();
            assertThat(sub.isSubscribed(), equalTo(false));
        }

        @Test
        void shouldResetInitializationWhenDisconnecting() {
            client.connection().connect();
            reset(requestStream);
            // when
            client.connection().disconnect();
            assertThat(client.isSessionInitialized(), equalTo(false));
            assertThat(client.isInitializationInProgress(), equalTo(false));
        }

        @Test
        void shouldScheduleReconnectionOnError() {
            client.connection().connect();
            verify(serviceStub, times(1)).subscribe(responseAdapterCaptor.capture());
            reset(dataEventLoop);
            // when
            responseAdapterCaptor.getValue().onError(new RuntimeException("test-case"));
            // then -- anyLong because of jitter
            verify(dataEventLoop, times(1))
                    .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
        }

        @Test
        void shouldDisconnectOnNonRetriableError() {
            client.connection().connect();
            verify(serviceStub, times(1)).subscribe(responseAdapterCaptor.capture());
            reset(dataEventLoop);
            // when
            responseAdapterCaptor.getValue().onError(new StatusRuntimeException(Status.UNAUTHENTICATED));
            // then -- anyLong because of jitter
            verifyNoInteractions(dataEventLoop);
            assertThat(client.isConnected(), equalTo(false));
            assertThat(client.isReconnect(), equalTo(false));
        }

        @Test
        void shouldNotReconnectWhenDisconnectCalledDuringBackoff() {
            client.connection().connect();
            verify(serviceStub, times(1)).subscribe(responseAdapterCaptor.capture());
            reset(dataEventLoop);
            // when
            responseAdapterCaptor.getValue().onError(new RuntimeException("test-case"));
            // capture reconnect job
            final ArgumentCaptor<Runnable> reconnectCaptor =
                    ArgumentCaptor.forClass(Runnable.class);
            verify(dataEventLoop, times(1))
                    .schedule(reconnectCaptor.capture(), anyLong(), eq(TimeUnit.MILLISECONDS));
            // when disconnect during backoff
            client.connection().disconnect();
            reconnectCaptor.getValue().run(); // reconnection fires
            assertThat(client.isInitializationInProgress(), equalTo(false));
        }
    }
}
