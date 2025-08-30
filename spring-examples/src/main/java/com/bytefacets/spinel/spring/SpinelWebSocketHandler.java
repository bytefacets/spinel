// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.spring;

import com.bytefacets.spinel.comms.SubscriptionConfig;
import com.bytefacets.spinel.comms.send.ConnectedSessionInfo;
import com.bytefacets.spinel.comms.send.SubscriptionContainer;
import com.bytefacets.spinel.comms.send.SubscriptionProvider;
import com.bytefacets.spinel.grpc.proto.CreateSubscription;
import com.bytefacets.spinel.grpc.proto.RequestType;
import com.bytefacets.spinel.grpc.proto.SubscriptionRequest;
import com.bytefacets.spinel.grpc.proto.SubscriptionResponse;
import com.bytefacets.spinel.grpc.send.GrpcEncoder;
import io.netty.channel.EventLoop;
import java.io.IOException;
import java.util.List;
import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class SpinelWebSocketHandler implements WebSocketHandler {
    private final SubscriptionProvider subscriptionProvider;
    private final ConnectedSessionInfo info = new SessionInfo("", "", "");
    private final EventLoop eventLoop;

    public SpinelWebSocketHandler(
            final SubscriptionProvider subscriptionProvider, final EventLoop eventLoop) {
        this.subscriptionProvider = subscriptionProvider;
        this.eventLoop = eventLoop;
    }

    @Override
    public Mono<Void> handle(final WebSocketSession session) {
        final Flux<SubscriptionRequest> input =
                session.receive()
                        .map(
                                msg -> {
                                    try {
                                        return SubscriptionRequest.parseFrom(
                                                msg.getPayload().asInputStream());
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                });

        return session.send(
                toOutput(input).map(response -> SpinelWebSocketHandler.encode(session, response)));
    }

    private Flux<SubscriptionResponse> toOutput(final Flux<SubscriptionRequest> requestFlux) {
        return requestFlux.flatMap(
                request -> {
                    if (request.getRequestType().equals(RequestType.REQUEST_TYPE_SUBSCRIBE)) {
                        return createSubscription(request);
                    } else {
                        return Flux.empty();
                    }
                });
    }

    private Publisher<SubscriptionResponse> createSubscription(final SubscriptionRequest request) {
        final CreateSubscription create = request.getSubscription();
        final var subscriptionConfig =
                SubscriptionConfig.subscriptionConfig(create.getName())
                        .defaultAll(create.getDefaultAll())
                        .build();
        final SubscriptionContainer sub =
                subscriptionProvider.getSubscription(info, subscriptionConfig, List.of());
        if (sub != null) {
            final GrpcEncoder encoder = GrpcEncoder.grpcEncoder(request.getSubscriptionId());
            return new FluxAdapter<>(encoder, sub.output()).flux(eventLoop);
        } else {
            throw new NullPointerException();
        }
    }

    private static WebSocketMessage encode(
            final WebSocketSession session, final SubscriptionResponse response) {
        try {
            return session.binaryMessage(
                    factory -> {
                        try {
                            final DataBuffer buffer =
                                    factory.allocateBuffer(response.getSerializedSize());
                            response.writeTo(buffer.asOutputStream());
                            return buffer;
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        } catch (Exception e) {
            throw new RuntimeException("Failed to encode SubscriptionResponse", e);
        }
    }

    private record SessionInfo(String user, String tenant, String remote)
            implements ConnectedSessionInfo {
        @Override
        public String getTenant() {
            return tenant;
        }

        @Override
        public String getUser() {
            return user;
        }

        @Override
        public String getRemote() {
            return remote;
        }
    }
}
