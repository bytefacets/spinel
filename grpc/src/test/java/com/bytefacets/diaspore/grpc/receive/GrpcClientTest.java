package com.bytefacets.diaspore.grpc.receive;

import static com.bytefacets.diaspore.schema.MatrixStoreFieldFactory.matrixStoreFieldFactory;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.bytefacets.diaspore.comms.ConnectionInfo;
import com.bytefacets.diaspore.comms.SubscriptionConfig;
import com.bytefacets.diaspore.grpc.proto.DataServiceGrpc;
import com.bytefacets.diaspore.grpc.proto.SubscriptionRequest;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.net.URI;
import java.util.concurrent.Executor;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GrpcClientTest {
    private final SchemaBuilder schemaBuilder =
            new SchemaBuilder(matrixStoreFieldFactory(16, 16, i -> {}));
    private final SubscriptionStore subscriptionStore = new SubscriptionStore();
    private @Mock ManagedChannel channel;
    private @Mock Executor dataExecutor;
    private @Mock DataServiceGrpc.DataServiceStub serviceStub;
    private @Mock Function<SchemaBuilder, GrpcDecoder> decoderSupplier;
    private @Mock GrpcDecoder decoder;
    private @Mock StreamObserver<SubscriptionRequest> requestStream;
    private GrpcClient client;

    @BeforeEach
    void setUp() {
        client =
                new GrpcClient(
                        new ConnectionInfo("test", URI.create("grpc://test")),
                        channel,
                        dataExecutor,
                        serviceStub,
                        decoderSupplier,
                        subscriptionStore);
        lenient().when(serviceStub.subscribe(any())).thenReturn(requestStream);
        lenient().when(decoderSupplier.apply(any())).thenReturn(decoder);
        lenient().when(channel.getState(false)).thenReturn(ConnectivityState.READY);
    }

    @Test
    void shouldCreateAndSendSubscriptionWhenConnected() {
        client.connection().connect();
        final var config = SubscriptionConfig.subscriptionConfig("foo").build();
        final var sub = client.createSubscription(schemaBuilder, config);
        assertThat(sub.isSubscribed(), equalTo(true));
        verify(requestStream, times(1)).onNext(sub.createRequest());
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
    }
}
