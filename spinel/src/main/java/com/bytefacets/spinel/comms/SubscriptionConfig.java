// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.comms;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.StringJoiner;

public final class SubscriptionConfig {
    private final String remoteOutputName;
    private final List<String> fields;
    private final boolean defaultAll;

    private SubscriptionConfig(
            final String remoteOutputName, final List<String> fields, final boolean defaultAll) {
        this.remoteOutputName = requireNonNull(remoteOutputName, "remoteOutputName");
        this.fields = requireNonNull(fields, "fields");
        this.defaultAll = defaultAll;
    }

    public String remoteOutputName() {
        return remoteOutputName;
    }

    public List<String> fields() {
        return fields;
    }

    /** Whether the subscription lets through all rows before any predicate is set. */
    public boolean defaultAll() {
        return defaultAll;
    }

    public static Builder subscriptionConfig(final String remoteOutputName) {
        return new Builder(remoteOutputName);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", SubscriptionConfig.class.getSimpleName() + "[", "]")
                .add("remoteOutputName='" + remoteOutputName + "'")
                .add("fields=" + fields)
                .add("defaultAll=" + defaultAll)
                .toString();
    }

    public static final class Builder {
        private final String remoteOutputName;
        private List<String> fields;
        private boolean defaultAll = false;

        private Builder(final String remoteOutputName) {
            this.remoteOutputName = remoteOutputName;
        }

        public Builder setFields(final List<String> fields) {
            this.fields = fields;
            return this;
        }

        /** Whether the subscription lets through all rows before any predicate is set. */
        public Builder defaultAll(final boolean defaultAll) {
            this.defaultAll = defaultAll;
            return this;
        }

        /** Lets all rows through the subscription before any predicate is set. */
        public Builder defaultAll() {
            this.defaultAll = true;
            return this;
        }

        /** Lets no rows through the subscription before any predicate is set. */
        public Builder defaultNone() {
            this.defaultAll = false;
            return this;
        }

        public SubscriptionConfig build() {
            final List<String> configFields = fields != null ? List.copyOf(fields) : List.of();
            return new SubscriptionConfig(remoteOutputName, configFields, defaultAll);
        }
    }
}
