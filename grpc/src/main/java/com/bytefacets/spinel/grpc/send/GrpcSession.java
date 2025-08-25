// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.send;

import static com.bytefacets.spinel.grpc.send.GrpcSink.grpcSink;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.hash.IntGenericIndexedMap;
import com.bytefacets.spinel.common.Connector;
import com.bytefacets.spinel.comms.SubscriptionConfig;
import com.bytefacets.spinel.comms.send.ConnectedSessionInfo;
import com.bytefacets.spinel.comms.send.ModificationResponse;
import com.bytefacets.spinel.comms.send.SubscriptionContainer;
import com.bytefacets.spinel.comms.send.SubscriptionProvider;
import com.bytefacets.spinel.comms.subscription.ModificationRequest;
import com.bytefacets.spinel.grpc.proto.CreateSubscription;
import com.bytefacets.spinel.grpc.proto.ModificationAddRemove;
import com.bytefacets.spinel.grpc.proto.RequestType;
import com.bytefacets.spinel.grpc.proto.Response;
import com.bytefacets.spinel.grpc.proto.ResponseType;
import com.bytefacets.spinel.grpc.proto.SubscriptionRequest;
import com.bytefacets.spinel.grpc.proto.SubscriptionResponse;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.channel.EventLoop;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class GrpcSession {
    private static final Logger log = LoggerFactory.getLogger(GrpcSession.class);
    private final ConnectedSessionInfo sessionInfo;
    private final SubscriptionProvider subscriptionProvider;
    private final EventLoop dataEventLoop;
    private final RequestHandler requestHandler = new RequestHandler();
    private final StreamObserver<SubscriptionResponse> outputStream;
    private final IntGenericIndexedMap<SubscriptionResources> subscriptions =
            new IntGenericIndexedMap<>(4);
    private final MsgHelp msgHelp = new MsgHelp();
    private final Consumer<GrpcSession> onComplete;
    private final SenderErrorEval errorEval;
    private final String logPrefix;

    static GrpcSession createSession(
            final ConnectedSessionInfo sessionInfo,
            final SubscriptionProvider subscriptionProvider,
            final ServerCallStreamObserver<SubscriptionResponse> outputStream,
            final EventLoop dataEventLoop,
            final Consumer<GrpcSession> onComplete) {
        return new GrpcSession(
                sessionInfo, subscriptionProvider, outputStream, dataEventLoop, onComplete);
    }

    GrpcSession(
            final ConnectedSessionInfo sessionInfo,
            final SubscriptionProvider subscriptionProvider,
            final ServerCallStreamObserver<SubscriptionResponse> outputStream,
            final EventLoop dataEventLoop,
            final Consumer<GrpcSession> onComplete) {
        this.subscriptionProvider = requireNonNull(subscriptionProvider, "subscriptionProvider");
        this.dataEventLoop = requireNonNull(dataEventLoop, "dataEventLoop");
        this.onComplete = requireNonNull(onComplete, "onComplete");
        this.outputStream = requireNonNull(outputStream, "outputStream");
        this.sessionInfo = requireNonNull(sessionInfo, "sessionInfo");
        this.logPrefix = sessionInfo.toString();
        this.errorEval = new SenderErrorEval(log, logPrefix);
        outputStream.setOnCancelHandler(this::cancelled);
    }

    StreamObserver<SubscriptionRequest> requestHandler() {
        return requestHandler;
    }

    void close() {
        dataEventLoop.execute(this::internalClose);
    }

    private void createSubscription(final SubscriptionRequest request) {
        final CreateSubscription createRequest = request.getSubscription();
        final SubscriptionConfig config = toConfig(createRequest);
        final List<ModificationRequest> initialModifications =
                msgHelp.readModifications(createRequest);
        try {
            final SubscriptionContainer subscriptionContainer =
                    subscriptionProvider.getSubscription(sessionInfo, config, initialModifications);
            if (subscriptionContainer != null) {
                final int subscriptionId = request.getSubscriptionId();
                final GrpcSink adapter = grpcSink(subscriptionId, outputStream);
                final var resources = new SubscriptionResources(subscriptionContainer, adapter);
                subscriptions.put(subscriptionId, resources);
                // connection to the output must be done on the data thread
                Connector.connectOutputToInput(subscriptionContainer, adapter);
            } else {
                outputStream.onNext(outputNotFound(request, createRequest.getName()));
            }
        } catch (Exception ex) {
            outputStream.onNext(error(request, createRequest.getName(), ex));
        }
    }

    private void modifySubscription(final SubscriptionRequest request) {
        final var subscriptionContainer =
                subscriptions.getOrDefault(request.getSubscriptionId(), null);
        try {
            if (subscriptionContainer != null) {
                final ModificationRequest modification = msgHelp.readModification(request);
                final ModificationResponse response =
                        subscriptionContainer.apply(
                                request.getModification().getAddRemove(), modification);
                outputStream.onNext(
                        messageResponse(request, response.success(), response.message()));
            } else {
                outputStream.onNext(subscriptionNotFound(request));
            }
        } catch (Exception ex) {
            outputStream.onNext(error(request, ex));
        }
    }

    // on data thread
    private void internalClose() {
        log.info("{} Closing session", logPrefix);
        subscriptions.forEachValue(SubscriptionResources::close);
        subscriptions.clear();
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
            log.debug(
                    "{} Received {} ({})",
                    logPrefix,
                    request.getRequestType(),
                    request.getMsgToken());
            if (request.getRequestType() == RequestType.REQUEST_TYPE_SUBSCRIBE) {
                createSubscription(request);
            } else if (request.getRequestType() == RequestType.REQUEST_TYPE_MODIFY) {
                modifySubscription(request);
            } else if (request.getRequestType() == RequestType.REQUEST_TYPE_INIT) {
                log.info(
                        "Initialization received: msg={}",
                        request.getInitialization().getMessage());
                outputStream.onNext(init(request.getMsgToken()));
            } else {
                onUnknownRequestType(request);
            }
        }

        private void onUnknownRequestType(final SubscriptionRequest request) {
            log.warn(
                    "{} Unknown RequestType received: {}",
                    logPrefix,
                    request.getRequestTypeValue());
            outputStream.onNext(invalidResponseType(request, request.getRequestTypeValue()));
        }

        @Override
        public void onError(final Throwable throwable) {
            errorEval.handleException(throwable);
        }

        @Override
        public void onCompleted() {
            log.info("{} Completed", logPrefix);
            onComplete.accept(GrpcSession.this);
        }
    }

    private static SubscriptionResponse messageResponse(
            final SubscriptionRequest req, final boolean isSuccess, final String message) {
        return SubscriptionResponse.newBuilder()
                .setRefMsgToken(req.getMsgToken())
                .setSubscriptionId(req.getSubscriptionId())
                .setResponseType(ResponseType.RESPONSE_TYPE_MESSAGE)
                .setResponse(
                        Response.newBuilder().setSuccess(isSuccess).setMessage(message).build())
                .build();
    }

    static SubscriptionResponse init(final int refToken) {
        return SubscriptionResponse.newBuilder()
                .setRefMsgToken(refToken)
                .setResponseType(ResponseType.RESPONSE_TYPE_INIT)
                .setResponse(Response.newBuilder().setSuccess(true).setMessage("hello").build())
                .build();
    }

    @VisibleForTesting
    int activeAdapters() {
        return subscriptions.size();
    }

    private static SubscriptionResponse outputNotFound(
            final SubscriptionRequest req, final String name) {
        return messageResponse(req, false, String.format("Output not found: %s", name));
    }

    private static SubscriptionResponse subscriptionNotFound(final SubscriptionRequest req) {
        return messageResponse(req, false, "Subscription not found");
    }

    private static SubscriptionResponse error(
            final SubscriptionRequest req, final String name, final Exception ex) {
        return messageResponse(
                req,
                false,
                String.format("Exception handling subscription for %s: %s", name, ex.getMessage()));
    }

    private static SubscriptionResponse error(final SubscriptionRequest req, final Exception ex) {
        return messageResponse(
                req, false, String.format("Exception modifying subscription: %s", ex.getMessage()));
    }

    private static SubscriptionResponse invalidResponseType(
            final SubscriptionRequest req, final int typeId) {
        return messageResponse(
                req, false, String.format("Request type not understood: %d", typeId));
    }

    private static SubscriptionConfig toConfig(final CreateSubscription msg) {
        final String name = msg.getName();
        final var builder =
                SubscriptionConfig.subscriptionConfig(name).defaultAll(msg.getDefaultAll());
        if (msg.getFieldNamesCount() > 0) {
            final List<String> fieldNames = new ArrayList<>(msg.getFieldNamesCount());
            for (int i = 0, len = msg.getFieldNamesCount(); i < len; i++) {
                fieldNames.add(msg.getFieldNames(i));
            }
            builder.setFields(fieldNames);
        }
        return builder.build();
    }

    private record SubscriptionResources(
            SubscriptionContainer subscriptionContainer, GrpcSink sink) {
        ModificationResponse apply(
                final ModificationAddRemove addRemove, final ModificationRequest descriptor) {
            if (addRemove.equals(ModificationAddRemove.ADD)) {
                return subscriptionContainer.add(descriptor);
            } else if (addRemove.equals(ModificationAddRemove.REMOVE)) {
                return subscriptionContainer.remove(descriptor);
            }
            return ModificationResponse.MODIFICATION_NOT_UNDERSTOOD;
        }

        void close() {
            subscriptionContainer.terminateSubscription();
            sink.close();
        }
    }
}
