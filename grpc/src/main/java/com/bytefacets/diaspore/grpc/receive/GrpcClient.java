package com.bytefacets.diaspore.grpc.receive;

import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.comms.ConnectionHandle;
import com.bytefacets.diaspore.comms.ConnectionInfo;
import com.bytefacets.diaspore.comms.SubscriptionConfig;
import com.bytefacets.diaspore.comms.receive.Receiver;
import com.bytefacets.diaspore.grpc.proto.DataServiceGrpc;
import com.bytefacets.diaspore.grpc.proto.ResponseType;
import com.bytefacets.diaspore.grpc.proto.SubscriptionRequest;
import com.bytefacets.diaspore.grpc.proto.SubscriptionResponse;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import io.netty.channel.EventLoop;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GrpcClient implements Receiver {
    private static final Logger log = LoggerFactory.getLogger(GrpcClient.class);
    private final ConnectionInfo connectionInfo;
    private final ManagedChannel channel;
    private final EventLoop dataEventLoop;
    private final DataServiceGrpc.DataServiceStub stub;
    private final ReceiveResponseAdapter responseAdapter = new ReceiveResponseAdapter();
    private final SendRequestAdapter requestAdapter = new SendRequestAdapter();
    private final SessionInitializer initializer = new SessionInitializer();
    private final ExponentialBackOff backOff = new ExponentialBackOff();
    private final AtomicInteger nextToken = new AtomicInteger(1);
    private final Function<SchemaBuilder, GrpcDecoder> decoderSupplier;
    private final SubscriptionStore subscriptionStore;
    private final ClientErrorEval errorEval;
    private final String logPrefix;

    GrpcClient(
            final ConnectionInfo connectionInfo,
            final ManagedChannel channel,
            final EventLoop dataEventLoop) {
        this(
                connectionInfo,
                channel,
                dataEventLoop,
                DataServiceGrpc.newStub(channel).withExecutor(dataEventLoop).withWaitForReady(),
                GrpcDecoder::grpcDecoder,
                new SubscriptionStore(connectionInfo));
    }

    GrpcClient(
            final ConnectionInfo connectionInfo,
            final ManagedChannel channel,
            final EventLoop dataEventLoop,
            final DataServiceGrpc.DataServiceStub stub,
            final Function<SchemaBuilder, GrpcDecoder> decoderSupplier,
            final SubscriptionStore subscriptionStore) {
        this.connectionInfo = requireNonNull(connectionInfo, "connectionInfo");
        this.channel = requireNonNull(channel, "channel");
        this.dataEventLoop = requireNonNull(dataEventLoop, "dataEventLoop");
        this.stub = requireNonNull(stub, "stub");
        this.decoderSupplier = requireNonNull(decoderSupplier, "decoderSupplier");
        this.subscriptionStore = requireNonNull(subscriptionStore, "subscriptionStore");
        this.logPrefix =
                String.format(
                        "ClientOf[name=%s,uri=%s]", connectionInfo.name(), connectionInfo.uri());
        this.errorEval = new ClientErrorEval(log, connectionInfo);
        requestAdapter.trackConnectionStateChange();
    }

    @Override
    public ConnectionHandle connection() {
        return requestAdapter;
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
        if (requestAdapter.requester != null) {
            sub.requestSubscriptionIfNecessary(this::issueSubscriptionRequest);
        }
        return sub;
    }

    private void issueSubscriptionRequest(final SubscriptionRequest request) {
        if (requestAdapter.requester != null) {
            log.info(
                    "{}} Issuing subscription request: token={}, name={}",
                    logPrefix,
                    request.getRefToken(),
                    request.getSubscription().getName());
            requestAdapter.requester.onNext(request);
        } else {
            log.warn("{} issueSubscriptionRequest called by requester is null", logPrefix);
        }
    }

    private class SendRequestAdapter implements ConnectionHandle {
        private final Object lock = new Object();
        private volatile boolean isClientSubscribed;
        private StreamObserver<SubscriptionRequest> requester;
        private volatile boolean reconnect;

        private void cleanUp() {
            synchronized (lock) {
                if (requester != null) {
                    log.info("{} Discontinuing client subscription", logPrefix);
                    requester.onCompleted();
                    requester = null;
                }
                subscriptionStore.resetSubscriptionStatus();
                isClientSubscribed = false;
                markSessionInitialized(false);
            }
        }

        private void reconnect() {
            if (reconnect) {
                connect();
            }
        }

        @Override
        public void disconnect() {
            reconnect = false;
            cleanUp();
        }

        @Override
        public void connect() {
            synchronized (lock) {
                reconnect = true;
                if (requester == null) {
                    log.info("{} Initiating client subscription", logPrefix);
                    markSessionInitialized(false);
                    requester = stub.subscribe(responseAdapter);
                    isClientSubscribed = true;
                }
                // in case already connected
                onNewChannelState(channel.getState(false));
            }
        }

        @Override
        public boolean isConnected() {
            return channel.getState(false).equals(ConnectivityState.READY) && isClientSubscribed;
        }

        private void onNewChannelState(final ConnectivityState newState) {
            if (newState.equals(ConnectivityState.READY) && isClientSubscribed) {
                backOff.reset();
                initializer.startIfNecessary();
            }
        }

        private void trackConnectionStateChange() {
            final ConnectivityState current = channel.getState(false);
            channel.notifyWhenStateChanged(
                    current,
                    () -> {
                        final ConnectivityState newState = channel.getState(false);
                        log.info("{} Client state change: {}", logPrefix, newState);
                        onNewChannelState(newState);
                        trackConnectionStateChange(); // listen for the next change
                    });
        }
    }

    /**
     * Because we're creating the stub with a specific executor, we'll always be called back in that
     * executor.
     */
    private class ReceiveResponseAdapter implements StreamObserver<SubscriptionResponse> {
        @Override
        public void onNext(final SubscriptionResponse response) {
            if (response.getResponseType().equals(ResponseType.RESPONSE_TYPE_INIT)) {
                initializer.receiveResponse(response);
            } else {
                subscriptionStore.accept(response);
            }
        }

        @Override
        public void onError(final Throwable t) {
            errorEval.handleException(t);
            requestAdapter.cleanUp();
            final long delay = backOff.nextDelayMillis();
            log.debug("{} scheduling reconnect in {} millis", logPrefix, delay);
            dataEventLoop.schedule(requestAdapter::reconnect, delay, TimeUnit.MILLISECONDS);
        }

        @Override
        public void onCompleted() {
            log.warn("{} gRPC completed", logPrefix);
        }
    }

    /**
     * When a connection is made, this initializer will exchange an initial message with the server
     * to establish readiness before sending the subscriptions.
     */
    private final class SessionInitializer implements Runnable {
        private final AtomicBoolean initializationInProgress = new AtomicBoolean();
        private final AtomicBoolean sessionInitialized = new AtomicBoolean();

        private void startIfNecessary() {
            if (!sessionInitialized.get() && initializationInProgress.compareAndSet(false, true)) {
                run();
            }
        }

        @Override
        public void run() {
            if (!sessionInitialized.get()) {
                sendInitialization();
                dataEventLoop.schedule(this, 1, TimeUnit.SECONDS);
            }
        }

        private void sendInitialization() {
            final int token = nextToken.getAndIncrement();
            log.debug(
                    "{} Requesting initialization: {} ({})",
                    logPrefix,
                    channel.getState(false),
                    token);
            requestAdapter.requester.onNext(MsgHelp.init(token, "anonymous"));
        }

        private void receiveResponse(final SubscriptionResponse response) {
            final String message = response.getResponse().getMessage();
            if (!response.getResponse().getError()) {
                markSessionInitialized(true);
                log.debug("{} Initialization response: {}", logPrefix, message);
                log.info(
                        "{} Resubscribing {} outputs if necessary",
                        logPrefix,
                        subscriptionStore.numSubscriptions());
                subscriptionStore.resubscribe(GrpcClient.this::issueSubscriptionRequest);
            } else {
                log.warn("{} initialization error response: {}", logPrefix, message);
            }
        }
    }

    @VisibleForTesting
    boolean isSessionInitialized() {
        return initializer.sessionInitialized.get();
    }

    @VisibleForTesting
    boolean isInitializationInProgress() {
        return initializer.initializationInProgress.get();
    }

    @VisibleForTesting
    void markSessionInitialized(final boolean value) {
        initializer.sessionInitialized.set(value);
        initializer.initializationInProgress.set(false);
    }
}
