package com.bytefacets.diaspore.comms;

import static java.util.Objects.requireNonNull;

import java.util.List;

public final class SubscriptionConfig {
    private final String remoteOutputName;
    private final List<String> fields;

    private SubscriptionConfig(final String remoteOutputName, final List<String> fields) {
        this.remoteOutputName = requireNonNull(remoteOutputName, "remoteOutputName");
        this.fields = requireNonNull(fields, "fields");
    }

    public String remoteOutputName() {
        return remoteOutputName;
    }

    public List<String> fields() {
        return fields;
    }

    public static Builder subscriptionConfig(final String remoteOutputName) {
        return new Builder(remoteOutputName);
    }

    public static class Builder {
        private final String remoteOutputName;
        private List<String> fields;

        private Builder(final String remoteOutputName) {
            this.remoteOutputName = remoteOutputName;
        }

        public Builder setFields(final List<String> fields) {
            this.fields = fields;
            return this;
        }

        public SubscriptionConfig build() {
            final List<String> configFields = fields != null ? List.copyOf(fields) : List.of();
            return new SubscriptionConfig(remoteOutputName, configFields);
        }
    }
}
