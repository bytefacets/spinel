// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.groupby;

import static com.bytefacets.diaspore.exception.FieldNotFoundException.fieldNotFound;

import com.bytefacets.diaspore.schema.ChangedFieldSet;
import com.bytefacets.diaspore.schema.Field;
import com.bytefacets.diaspore.schema.FieldResolver;
import com.bytefacets.diaspore.schema.Schema;
import com.bytefacets.diaspore.schema.SchemaField;
import java.util.BitSet;
import javax.annotation.Nullable;

final class GroupFunctionBinding {
    private final BitSet inboundFieldReferences = new BitSet();
    private final Resolver resolver = new Resolver();

    void bind(final Schema inSchema, final GroupFunction function) {
        resolver.schema = inSchema;
        inboundFieldReferences.clear();
        function.bindToSchema(resolver);
        resolver.schema = null;
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
            inboundFieldReferences.set(field.fieldId());
            return field.field();
        }
    }
}
