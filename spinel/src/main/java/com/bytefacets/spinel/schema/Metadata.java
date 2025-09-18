// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.schema;

import static java.util.Objects.requireNonNull;

import jakarta.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

public final class Metadata {
    public static final Metadata EMPTY = new Metadata(Set.of(), Map.of());
    private final Set<String> tags;
    private final Map<String, Object> attributes;

    public static Metadata metadata(final Set<String> tags, final Map<String, Object> attributes) {
        return new Metadata(
                tags != null ? Set.copyOf(tags) : Set.of(),
                attributes != null ? Map.copyOf(attributes) : Map.of());
    }

    public static Metadata metadata(final Set<String> tags) {
        return metadata(tags, null);
    }

    public static Metadata metadata(final Map<String, Object> attributes) {
        return metadata(null, attributes);
    }

    private Metadata(final Set<String> tags, final Map<String, Object> attributes) {
        this.tags = requireNonNull(tags, "tags");
        this.attributes = requireNonNull(attributes, "attributes");
    }

    public Set<String> tags() {
        if (tags == null) {
            return Set.of();
        } else {
            return Collections.unmodifiableSet(tags);
        }
    }

    public Map<String, Object> attributes() {
        return attributes;
    }

    public boolean hasTag(final String tag) {
        return tags.contains(tag);
    }

    public @Nullable Object getAttribute(final String name) {
        return attributes.get(name);
    }

    @SuppressWarnings("NeedBraces")
    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Metadata metadata = (Metadata) o;
        return Objects.equals(tags, metadata.tags)
                && Objects.equals(attributes, metadata.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tags, attributes);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", Metadata.class.getSimpleName() + "[", "]")
                .add("tags=" + tags)
                .add("attributes=" + attributes)
                .toString();
    }
}
