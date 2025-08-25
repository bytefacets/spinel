// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.receive;

import com.bytefacets.spinel.comms.SubscriptionConfig;
import com.bytefacets.spinel.comms.subscription.ModificationRequest;
import com.bytefacets.spinel.grpc.codec.ObjectEncoderImpl;
import com.bytefacets.spinel.grpc.proto.CreateSubscription;
import com.bytefacets.spinel.grpc.proto.InitializationRequest;
import com.bytefacets.spinel.grpc.proto.ModificationAddRemove;
import com.bytefacets.spinel.grpc.proto.ModifySubscription;
import com.bytefacets.spinel.grpc.proto.RequestType;
import com.bytefacets.spinel.grpc.proto.SubscriptionRequest;
import java.util.List;

final class MsgHelp {
    private final ObjectEncoderImpl encoder = ObjectEncoderImpl.encoder();
    private final ModifySubscription.Builder modifyBuilder = ModifySubscription.newBuilder();
    private int nextToken = 1;

    SubscriptionRequest request(final int subscriptionId, final CreateSubscription payload) {
        return SubscriptionRequest.newBuilder()
                .setMsgToken(nextToken++)
                .setSubscriptionId(subscriptionId)
                .setRequestType(RequestType.REQUEST_TYPE_SUBSCRIBE)
                .setSubscription(payload)
                .build();
    }

    SubscriptionRequest addModification(
            final int subscriptionId, final ModificationRequest request) {
        return toRequest(subscriptionId, modify(request, ModificationAddRemove.ADD));
    }

    SubscriptionRequest removeModification(
            final int subscriptionId, final ModificationRequest request) {
        return toRequest(subscriptionId, modify(request, ModificationAddRemove.REMOVE));
    }

    private SubscriptionRequest toRequest(
            final int subscriptionId, final ModifySubscription.Builder builder) {
        return SubscriptionRequest.newBuilder()
                .setMsgToken(nextToken++)
                .setSubscriptionId(subscriptionId)
                .setRequestType(RequestType.REQUEST_TYPE_MODIFY)
                .setModification(builder)
                .build();
    }

    ModifySubscription.Builder modify(
            final ModificationRequest request, final ModificationAddRemove addRemove) {
        modifyBuilder.clear();
        modifyBuilder
                .setAction(request.action())
                .setTarget(request.target())
                .setAddRemove(addRemove);
        if (request.arguments() != null && request.arguments().length > 0) {
            for (int i = 0, len = request.arguments().length; i < len; i++) {
                final Object arg = request.arguments()[i];
                modifyBuilder.addArguments(encoder.encode(arg));
            }
        }
        return modifyBuilder;
    }

    CreateSubscription subscription(
            final SubscriptionConfig config, final List<ModificationRequest> initialRequests) {
        final var builder = CreateSubscription.newBuilder().setName(config.remoteOutputName());
        if (config.fields() != null && !config.fields().isEmpty()) {
            builder.addAllFieldNames(config.fields());
        }
        builder.setDefaultAll(config.defaultAll());
        for (int i = 0, len = initialRequests.size(); i < len; i++) {
            builder.addModifications(
                    i, modify(initialRequests.get(i), ModificationAddRemove.ADD).build());
        }
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
