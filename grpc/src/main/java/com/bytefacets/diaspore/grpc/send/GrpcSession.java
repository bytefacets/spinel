package com.bytefacets.diaspore.grpc.send;

import static com.bytefacets.diaspore.grpc.send.GrpcSink.grpcSink;
import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.TransformOutput;
import com.bytefacets.diaspore.comms.send.OutputRegistry;
import com.bytefacets.diaspore.grpc.proto.CreateSubscription;
import com.bytefacets.diaspore.grpc.proto.RequestType;
import com.bytefacets.diaspore.grpc.proto.Response;
import com.bytefacets.diaspore.grpc.proto.ResponseType;
import com.bytefacets.diaspore.grpc.proto.SubscriptionRequest;
import com.bytefacets.diaspore.grpc.proto.SubscriptionResponse;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.channel.EventLoop;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class GrpcSession {
    private static final Logger log = LoggerFactory.getLogger(GrpcSession.class);
    private final OutputRegistry registry;
    private final EventLoop dataEventLoop;
    private final RequestHandler requestHandler = new RequestHandler();
    private final StreamObserver<SubscriptionResponse> outputStream;
    private final Map<String, GrpcSink> activeAdapters = new HashMap<>(4);
    private final Consumer<GrpcSession> onComplete;
    private final SenderErrorEval errorEval;

    static GrpcSession createSession(
            final OutputRegistry registry,
            final ServerCallStreamObserver<SubscriptionResponse> outputStream,
            final EventLoop dataEventLoop,
            final Consumer<GrpcSession> onComplete) {
        return new GrpcSession(registry, outputStream, dataEventLoop, onComplete);
    }

    GrpcSession(
            final OutputRegistry registry,
            final ServerCallStreamObserver<SubscriptionResponse> outputStream,
            final EventLoop dataEventLoop,
            final Consumer<GrpcSession> onComplete) {
        this.registry = requireNonNull(registry, "registry");
        this.dataEventLoop = requireNonNull(dataEventLoop, "dataEventLoop");
        this.onComplete = requireNonNull(onComplete, "onComplete");
        this.outputStream = requireNonNull(outputStream, "outputStream");
        this.errorEval = new SenderErrorEval(log);
        outputStream.setOnCancelHandler(this::cancelled);
    }

    StreamObserver<SubscriptionRequest> requestHandler() {
        return requestHandler;
    }

    void close() {
        dataEventLoop.execute(this::internalClose);
    }

    // on data thread
    private void subscribeOnDataThread(final SubscriptionRequest request) {
        final CreateSubscription subscription = request.getSubscription();
        final String name = subscription.getName();
        final TransformOutput output = registry.lookup(name);
        if (output != null) {
            final GrpcSink adapter = grpcSink(request.getRefToken(), outputStream);
            activeAdapters.put(name, adapter);
            // connection to the output must be done on the data thread
            output.attachInput(adapter.input());
        } else {
            outputStream.onNext(outputNotFound(request.getRefToken(), name));
        }
    }

    // on data thread
    private void internalClose() {
        log.info("Closing session");
        activeAdapters.values().forEach(GrpcSink::close);
        activeAdapters.clear();
    }

    private void cancelled() {
        log.info("Client cancelled connection");
    }

    /**
     * Server should be created with executor event loop corresponding to the data thread, so all
     * callbacks will be on the data event loop.
     */
    private class RequestHandler implements StreamObserver<SubscriptionRequest> {
        @Override
        public void onNext(final SubscriptionRequest request) {
            log.debug("Received {} ({})", request.getRequestType(), request.getRefToken());
            if (request.getRequestType() == RequestType.REQUEST_TYPE_SUBSCRIBE) {
                subscribeOnDataThread(request);
            } else if (request.getRequestType() == RequestType.REQUEST_TYPE_INIT) {
                log.info("Initialization received: user={}", request.getInitialization().getUser());
                outputStream.onNext(init(request.getRefToken()));
            } else {
                onUnknownRequestType(request);
            }
        }

        private void onUnknownRequestType(final SubscriptionRequest request) {
            log.warn("Unknown RequestType received: {}", request.getRequestTypeValue());
            outputStream.onNext(
                    invalidResponseType(request.getRefToken(), request.getRequestTypeValue()));
        }

        @Override
        public void onError(final Throwable throwable) {
            errorEval.handleException(throwable);
        }

        @Override
        public void onCompleted() {
            log.info("Completed");
            onComplete.accept(GrpcSession.this);
        }
    }

    private static SubscriptionResponse messageResponse(
            final int refToken, final boolean isError, final String message) {
        return SubscriptionResponse.newBuilder()
                .setRefToken(refToken)
                .setResponseType(ResponseType.RESPONSE_TYPE_MESSAGE)
                .setResponse(Response.newBuilder().setError(isError).setMessage(message).build())
                .build();
    }

    static SubscriptionResponse init(final int refToken) {
        return SubscriptionResponse.newBuilder()
                .setRefToken(refToken)
                .setResponseType(ResponseType.RESPONSE_TYPE_INIT)
                .setResponse(Response.newBuilder().setError(false).setMessage("hello").build())
                .build();
    }

    @VisibleForTesting
    int activeAdapters() {
        return activeAdapters.size();
    }

    private static SubscriptionResponse outputNotFound(final int refToken, final String name) {
        return messageResponse(refToken, true, String.format("Output not found: %s", name));
    }

    private static SubscriptionResponse invalidResponseType(final int refToken, final int typeId) {
        return messageResponse(
                refToken, true, String.format("Request type not understood: %d", typeId));
    }
}
