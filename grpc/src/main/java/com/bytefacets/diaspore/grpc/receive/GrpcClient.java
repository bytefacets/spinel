package com.bytefacets.diaspore.grpc.receive;

import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.comms.ConnectionHandle;
import com.bytefacets.diaspore.comms.ConnectionInfo;
import com.bytefacets.diaspore.comms.SubscriptionConfig;
import com.bytefacets.diaspore.comms.receive.Receiver;
import com.bytefacets.diaspore.grpc.proto.DataServiceGrpc;
import com.bytefacets.diaspore.grpc.proto.SubscriptionRequest;
import com.bytefacets.diaspore.grpc.proto.SubscriptionResponse;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GrpcClient implements Receiver {
    private static final Logger log = LoggerFactory.getLogger(GrpcClient.class);
    private final ConnectionInfo connectionInfo;
    private final ManagedChannel channel;
    private final DataServiceGrpc.DataServiceStub stub;
    private final ReceiveResponseAdapter handler = new ReceiveResponseAdapter();
    private final SendRequestAdapter handle = new SendRequestAdapter();
    private final AtomicInteger nextToken = new AtomicInteger(1);
    private final Function<SchemaBuilder, GrpcDecoder> decoderSupplier;
    private final SubscriptionStore subscriptionStore;
    private final Executor dataExecutor;

    GrpcClient(
            final ConnectionInfo connectionInfo,
            final ManagedChannel channel,
            final Executor dataExecutor) {
        this(
                connectionInfo,
                channel,
                dataExecutor,
                DataServiceGrpc.newStub(channel),
                GrpcDecoder::grpcDecoder,
                new SubscriptionStore());
    }

    GrpcClient(
            final ConnectionInfo connectionInfo,
            final ManagedChannel channel,
            final Executor dataExecutor,
            final DataServiceGrpc.DataServiceStub stub,
            final Function<SchemaBuilder, GrpcDecoder> decoderSupplier,
            final SubscriptionStore subscriptionStore) {
        this.connectionInfo = requireNonNull(connectionInfo, "connectionInfo");
        this.channel = requireNonNull(channel, "channel");
        this.dataExecutor = requireNonNull(dataExecutor, "dataExecutor");
        this.stub = requireNonNull(stub, "stub");
        this.decoderSupplier = requireNonNull(decoderSupplier, "decoderSupplier");
        this.subscriptionStore = requireNonNull(subscriptionStore, "subscriptionStore");
    }

    @Override
    public ConnectionHandle connection() {
        return handle;
    }

    @Override
    public ConnectionInfo connectionInfo() {
        return connectionInfo;
    }

    Subscription createSubscription(
            final SchemaBuilder schemaBuilder, final SubscriptionConfig config) {
        final int token = nextToken.getAndIncrement();
        final GrpcDecoder decoder = decoderSupplier.apply(schemaBuilder);
        final var sub = subscriptionStore.createSubscription(token, decoder, config);
        if (handle.requester != null) {
            sub.requestSubscriptionIfNecessary(this::issueSubscriptionRequest);
        }
        return sub;
    }

    private void issueSubscriptionRequest(final SubscriptionRequest request) {
        if (handle.requester != null) {
            log.info(
                    "Issuing subscription request: token={}, name={}",
                    request.getRefToken(),
                    request.getSubscription().getName());
            handle.requester.onNext(request);
        }
    }

    private class SendRequestAdapter implements ConnectionHandle {
        private final Object lock = new Object();
        private volatile boolean isClientSubscribed;
        private StreamObserver<SubscriptionRequest> requester;

        @Override
        public void disconnect() {
            synchronized (lock) {
                if (requester != null) {
                    log.info("Discontinuing client subscription: {}", connectionInfo);
                    requester.onCompleted();
                    requester = null;
                }
                subscriptionStore.resetSubscriptionStatus();
                isClientSubscribed = false;
            }
        }

        @Override
        public void connect() {
            synchronized (lock) {
                if (requester == null) {
                    log.info("Initiating client subscription: {}", connectionInfo);
                    requester = stub.subscribe(handler);
                    isClientSubscribed = true;
                    subscriptionStore.resubscribe(GrpcClient.this::issueSubscriptionRequest);
                }
            }
        }

        @Override
        public boolean isConnected() {
            return channel.getState(false).equals(ConnectivityState.READY) && isClientSubscribed;
        }
    }

    private class ReceiveResponseAdapter implements StreamObserver<SubscriptionResponse> {
        private final ConcurrentLinkedDeque<SubscriptionResponse> receivedResponses =
                new ConcurrentLinkedDeque<>();

        // on grpc thread
        @Override
        public void onNext(final SubscriptionResponse response) {
            receivedResponses.addLast(response);
            dataExecutor.execute(this::processPending);
        }

        // on grpc thread
        @Override
        public void onError(final Throwable t) {
            log.warn("Error on {}", connectionInfo, t);
        }

        // on grpc thread
        @Override
        public void onCompleted() {
            log.warn("Client gRPC completed");
        }

        // on data thread
        private void processPending() {
            while (!receivedResponses.isEmpty()) {
                subscriptionStore.accept(receivedResponses.removeFirst());
            }
        }
    }
}
