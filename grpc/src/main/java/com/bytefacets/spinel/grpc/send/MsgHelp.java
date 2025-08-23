// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.send;

import com.bytefacets.spinel.comms.subscription.ChangeDescriptor;
import com.bytefacets.spinel.comms.subscription.ModificationRequest;
import com.bytefacets.spinel.grpc.codec.ObjectDecoderRegistry;
import com.bytefacets.spinel.grpc.proto.ModifySubscription;
import com.bytefacets.spinel.grpc.proto.SubscriptionRequest;

final class MsgHelp {
    private static final Object[] EMPTY_ARGS = new Object[0];

    ModificationRequest readModification(final SubscriptionRequest request) {
        return buildModification(request.getModification());
    }

    private ModificationRequest buildModification(final ModifySubscription modification) {
        final String target = modification.getTarget();
        final String action = modification.getAction();
        final int argCount = modification.getArgumentsCount();
        if (argCount != 0) {
            final Object[] args = new Object[argCount];
            for (int i = 0; i < argCount; i++) {
                args[i] = ObjectDecoderRegistry.decode(modification.getArguments(i));
            }
            return ChangeDescriptor.change(target, action, args);
        } else {
            return ChangeDescriptor.change(target, action, EMPTY_ARGS);
        }
    }
}
