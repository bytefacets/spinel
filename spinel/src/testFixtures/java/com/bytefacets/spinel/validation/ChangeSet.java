// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.validation;

import com.bytefacets.spinel.schema.Metadata;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public final class ChangeSet {
    final Map<Key, RowData> added = new HashMap<>();
    final Map<Key, RowData> changed = new HashMap<>();
    final Set<Key> removed = new HashSet<>();
    final Map<String, Class<?>> schema = new HashMap<>();
    final Map<String, Metadata> metadata = new HashMap<>();
    boolean nullSchema = false;
    final LinkedHashSet<String> errors = new LinkedHashSet<>();

    ChangeSet() {}

    public ChangeSet nullSchema() {
        this.nullSchema = true;
        return this;
    }

    public ChangeSet schema(final Map<String, Class<?>> fieldMap) {
        schema.putAll(fieldMap);
        return this;
    }

    public ChangeSet metadata(final Map<String, Metadata> metadata) {
        this.metadata.putAll(metadata);
        return this;
    }

    public ChangeSet added(final Key key, final RowData row) {
        added.put(key, row);
        return this;
    }

    public ChangeSet changed(final Key key, final RowData row) {
        changed.put(key, row);
        return this;
    }

    public ChangeSet removed(final Key key) {
        removed.add(key);
        return this;
    }

    public ChangeSet error(final String message) {
        errors.add(message);
        return this;
    }
}
