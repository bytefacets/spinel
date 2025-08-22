// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.interner;

import static com.bytefacets.spinel.interner.InternSetProvider.internSetProvider;
import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.schema.Field;
import com.bytefacets.spinel.schema.FieldResolver;
import java.util.List;

public final class DynamicRowInterner implements RowInterner {
    private final List<String> fieldNames;
    private final int initialCapacity;
    private final InternSetProvider setProvider;
    private RowInterner resolvedInterner;

    public static DynamicRowInterner dynamicRowInterner(
            final List<String> fieldNames, final int initialCapacity) {
        return new DynamicRowInterner(fieldNames, initialCapacity, internSetProvider());
    }

    DynamicRowInterner(
            final List<String> fieldNames,
            final int initialCapacity,
            final InternSetProvider setProvider) {
        this.fieldNames = List.copyOf(fieldNames);
        this.initialCapacity = initialCapacity;
        this.setProvider = requireNonNull(setProvider, "setProvider");
    }

    @Override
    public int intern(final int row) {
        return resolvedInterner.intern(row);
    }

    @Override
    public void freeEntry(final int entry) {
        resolvedInterner.freeEntry(entry);
    }

    @Override
    public void bindToSchema(final FieldResolver fieldResolver) {
        if (fieldNames.isEmpty()) {
            resolvedInterner = ConstantRowInterner.Instance;
        } else if (fieldNames.size() == 1) {
            resolvedInterner = singleFieldInterner(fieldNames.get(0), fieldResolver);
        } else {
            resolvedInterner =
                    new TupleInterner(fieldNames, setProvider.getOrCreateTupleSet(initialCapacity));
        }
        resolvedInterner.bindToSchema(fieldResolver);
    }

    private RowInterner singleFieldInterner(
            final String fieldName, final FieldResolver fieldResolver) {
        final Field field = fieldResolver.getField(fieldName);
        return InternerFactory.interner(fieldName, field.typeId(), initialCapacity, setProvider);
    }

    @Override
    public void unbindSchema() {
        if (resolvedInterner != null) {
            resolvedInterner.unbindSchema();
            resolvedInterner = null;
        }
        setProvider.reset();
    }
}
