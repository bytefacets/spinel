// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.comms.subscription;

import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;

public final class ChangeDescriptor implements ModificationRequest {
    private final String target;
    private final String action;
    private final Object[] arguments;

    public static ChangeDescriptor change(
            final String target, final String action, final Object[] arguments) {
        return new ChangeDescriptor(target, action, arguments);
    }

    ChangeDescriptor(final String target, final String action, final Object[] arguments) {
        this.target = target;
        this.action = action;
        this.arguments = arguments;
    }

    @Override
    public String target() {
        return target;
    }

    @Override
    public String action() {
        return action;
    }

    @Override
    public Object[] arguments() {
        return arguments;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ChangeDescriptor.class.getSimpleName() + "[", "]")
                .add("target='" + target + "'")
                .add("action='" + action + "'")
                .add("arguments=" + Arrays.toString(arguments))
                .toString();
    }

    @SuppressWarnings("NeedBraces")
    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ChangeDescriptor that = (ChangeDescriptor) o;
        return Objects.equals(target, that.target)
                && Objects.equals(action, that.action)
                && Objects.deepEquals(arguments, that.arguments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(target, action, Arrays.hashCode(arguments));
    }
}
