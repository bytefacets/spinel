// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.comms.subscription;

public final class ChangeDescriptorFactory {
    public static final class Action {
        public static final String ADD = "add";
        public static final String REMOVE = "remove";

        private Action() {}
    }

    public static final class Target {
        public static final String FILTER = "filter";

        private Target() {}
    }

    public static ChangeDescriptor addPredicate(final String expression) {
        return ChangeDescriptor.change(Target.FILTER, Action.ADD, new Object[] {expression});
    }

    public static ChangeDescriptor removePredicate(final String expression) {
        return ChangeDescriptor.change(Target.FILTER, Action.REMOVE, new Object[] {expression});
    }
}
