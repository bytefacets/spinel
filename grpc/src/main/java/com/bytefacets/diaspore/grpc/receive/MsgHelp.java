package com.bytefacets.diaspore.grpc.receive;

import com.bytefacets.diaspore.comms.SubscriptionConfig;
import com.bytefacets.diaspore.grpc.proto.CreateSubscription;
import com.bytefacets.diaspore.grpc.proto.InitializationRequest;
import com.bytefacets.diaspore.grpc.proto.RequestType;
import com.bytefacets.diaspore.grpc.proto.SubscriptionRequest;

final class MsgHelp {
    private int nextToken = 1;

    SubscriptionRequest request(final int subscriptionId, final CreateSubscription payload) {
        return SubscriptionRequest.newBuilder()
                .setRefToken(nextToken++)
                .setSubscriptionId(subscriptionId)
                .setRequestType(RequestType.REQUEST_TYPE_SUBSCRIBE)
                .setSubscription(payload)
                .build();
    }

    CreateSubscription subscription(final SubscriptionConfig config) {
        final var builder = CreateSubscription.newBuilder().setName(config.remoteOutputName());
        if (config.fields() != null && !config.fields().isEmpty()) {
            builder.addAllFieldNames(config.fields());
        }
        builder.setDefaultAll(config.defaultAll());
        return builder.build();
    }

    SubscriptionRequest init(final String message) {
        return SubscriptionRequest.newBuilder()
                .setRefToken(nextToken++)
                .setRequestType(RequestType.REQUEST_TYPE_INIT)
                .setInitialization(InitializationRequest.newBuilder().setMessage(message).build())
                .build();
    }
}
