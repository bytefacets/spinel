// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.groupby;

import static com.bytefacets.spinel.exception.FieldNotFoundException.fieldNotFound;

import com.bytefacets.spinel.interner.RowInterner;
import com.bytefacets.spinel.schema.ChangedFieldSet;
import com.bytefacets.spinel.schema.Field;
import com.bytefacets.spinel.schema.FieldResolver;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.SchemaField;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

final class GroupFunctionBinding {
    private final BitSet inboundFieldReferences = new BitSet();
    private final Resolver resolver = new Resolver();
    private final List<String> fieldNames = new ArrayList<>(1);

    void bind(final Schema inSchema, final RowInterner function) {
        fieldNames.clear();
        resolver.schema = inSchema;
        inboundFieldReferences.clear();
        function.bindToSchema(resolver);
        resolver.schema = null;
    }

    List<String> fieldNames() {
        return fieldNames;
    }

    boolean isChanged(final ChangedFieldSet inChanges) {
        return inChanges.intersects(inboundFieldReferences);
    }

    private final class Resolver implements FieldResolver {
        private Schema schema;

        @Nullable
        @Override
        public Field findField(final String name) {
            final SchemaField field = schema.maybeField(name);
            if (field == null) {
                throw fieldNotFound(name, "GroupFunction", schema.name());
            }
            fieldNames.add(name);
            inboundFieldReferences.set(field.fieldId());
            return field.field();
        }
    }
}
