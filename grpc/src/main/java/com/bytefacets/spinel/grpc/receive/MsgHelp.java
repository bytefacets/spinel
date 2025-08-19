// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.receive;

import com.bytefacets.spinel.comms.SubscriptionConfig;
import com.bytefacets.spinel.grpc.proto.CreateSubscription;
import com.bytefacets.spinel.grpc.proto.InitializationRequest;
import com.bytefacets.spinel.grpc.proto.RequestType;
import com.bytefacets.spinel.grpc.proto.SubscriptionRequest;

final class MsgHelp {
    private int nextToken = 1;

    SubscriptionRequest request(final int subscriptionId, final CreateSubscription payload) {
        return SubscriptionRequest.newBuilder()
                .setMsgToken(nextToken++)
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
                .setMsgToken(nextToken++)
                .setRequestType(RequestType.REQUEST_TYPE_INIT)
                .setInitialization(InitializationRequest.newBuilder().setMessage(message).build())
                .build();
    }
}
