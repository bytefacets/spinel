package com.bytefacets.diaspore.comms;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.StringJoiner;

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

    @Override
    public String toString() {
        return new StringJoiner(", ", SubscriptionConfig.class.getSimpleName() + "[", "]")
                .add("remoteOutputName='" + remoteOutputName + "'")
                .add("fields=" + fields)
                .toString();
    }

    public static final class Builder {
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
