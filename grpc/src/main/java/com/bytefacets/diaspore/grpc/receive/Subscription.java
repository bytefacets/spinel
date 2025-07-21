package com.bytefacets.diaspore.grpc.receive;

import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.comms.SubscriptionConfig;
import com.bytefacets.diaspore.grpc.proto.SubscriptionRequest;

final class Subscription {
    private final int token;
    private final SubscriptionConfig config;
    private final GrpcDecoder decoder;

    Subscription(final int token, final GrpcDecoder decoder, final SubscriptionConfig config) {
        this.token = token;
        this.decoder = requireNonNull(decoder, "decoder");
        this.config = requireNonNull(config, "config");
    }

    SubscriptionConfig config() {
        return config;
    }

    GrpcDecoder decoder() {
        return decoder;
    }

    SubscriptionRequest createRequest() {
        return MsgHelp.request(token, MsgHelp.subscription(config));
    }
}
