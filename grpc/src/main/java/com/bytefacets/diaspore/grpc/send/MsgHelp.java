// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.grpc.send;

import com.bytefacets.diaspore.comms.subscription.ChangeDescriptor;
import com.bytefacets.diaspore.comms.subscription.ModificationRequest;
import com.bytefacets.diaspore.grpc.proto.ModifySubscription;
import com.bytefacets.diaspore.grpc.proto.SubscriptionRequest;
import com.bytefacets.diaspore.grpc.receive.ObjectDecoderRegistry;

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
