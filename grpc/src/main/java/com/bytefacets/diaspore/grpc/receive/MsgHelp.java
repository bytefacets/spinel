package com.bytefacets.diaspore.grpc.receive;

import com.bytefacets.diaspore.comms.SubscriptionConfig;
import com.bytefacets.diaspore.grpc.proto.CreateSubscription;
import com.bytefacets.diaspore.grpc.proto.InitializationRequest;
import com.bytefacets.diaspore.grpc.proto.RequestType;
import com.bytefacets.diaspore.grpc.proto.SubscriptionRequest;

final class MsgHelp {
    private MsgHelp() {}

    static SubscriptionRequest request(final int token, final CreateSubscription payload) {
        return SubscriptionRequest.newBuilder()
                .setRefToken(token)
                .setRequestType(RequestType.REQUEST_TYPE_SUBSCRIBE)
                .setSubscription(payload)
                .build();
    }

    static CreateSubscription subscription(final SubscriptionConfig config) {
        final var builder = CreateSubscription.newBuilder().setName(config.remoteOutputName());
        if (config.fields() != null && !config.fields().isEmpty()) {
            builder.addAllFieldNames(config.fields());
        }
        return builder.build();
    }

    static SubscriptionRequest init(final int token, final String message) {
        return SubscriptionRequest.newBuilder()
                .setRefToken(token)
                .setRequestType(RequestType.REQUEST_TYPE_INIT)
                .setInitialization(InitializationRequest.newBuilder().setMessage(message).build())
                .build();
    }
}
