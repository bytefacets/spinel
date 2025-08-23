// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.comms.subscription;

import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;

public final class ModificationRequestFactory {
    public static final class Action {
        public static final String APPLY = "apply";

        private Action() {}
    }

    public static final class Target {
        public static final String FILTER = "filter";

        private Target() {}
    }

    private ModificationRequestFactory() {}

    /**
     * Creates a ModificationRequest that targets the "filter" of a SubscriptionContainer, such as
     * the {@link com.bytefacets.spinel.comms.send.DefaultSubscriptionContainer}.
     */
    public static ModificationRequest applyFilterExpression(final String expression) {
        return new Impl(Target.FILTER, Action.APPLY, new Object[] {expression});
    }

    private record Impl(String target, String action, Object[] arguments)
            implements ModificationRequest {
        @SuppressWarnings("NeedBraces")
        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (!(o instanceof final ModificationRequest impl)) return false;
            return Objects.equals(target, impl.target())
                    && Objects.equals(action, impl.action())
                    && Objects.deepEquals(arguments, impl.arguments());
        }

        @Override
        public int hashCode() {
            return Objects.hash(target, action, Arrays.hashCode(arguments));
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", Impl.class.getSimpleName() + "[", "]")
                    .add("target='" + target + "'")
                    .add("action='" + action + "'")
                    .add("arguments=" + Arrays.toString(arguments))
                    .toString();
        }
    }
}
