// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.projection;

import static com.bytefacets.diaspore.exception.FieldNotFoundException.fieldNotFound;

import com.bytefacets.collections.arrays.GenericArray;
import com.bytefacets.collections.arrays.IntArray;
import com.bytefacets.diaspore.schema.ChangedFieldSet;
import com.bytefacets.diaspore.schema.Field;
import com.bytefacets.diaspore.schema.FieldResolver;
import com.bytefacets.diaspore.schema.Schema;
import com.bytefacets.diaspore.schema.SchemaField;
import java.util.BitSet;
import java.util.Map;

class ProjectionDependencyMap {
    private BitSet[] inboundFieldReferences = GenericArray.create(BitSet.class, 8);
    private int[] outboundToInboundMap = IntArray.create(8, -1);
    private final Resolver resolver = new Resolver();

    void reset() {
        for (BitSet set : inboundFieldReferences) {
            if (set != null) {
                set.clear();
            }
        }
    }

    void unbindCalculatedFields(final Map<String, CalculatedFieldDescriptor> newCalcs) {
        newCalcs.forEach((name, calc) -> calc.calculation().unbindSchema());
    }

    void bindCalculatedFields(
            final Schema inboundSchema,
            final Schema outboundSchema,
            final Map<String, CalculatedFieldDescriptor> newCalcs) {
        resolver.reset(inboundSchema, outboundSchema);
        for (var entry : newCalcs.entrySet()) {
            final String fieldName = entry.getKey();
            resolver.outboundFieldId = outboundSchema.field(fieldName).fieldId();
            entry.getValue().calculation().bindToSchema(resolver);
        }
    }

    void translateInboundChangeFields(
            final ChangedFieldSet inboundChanges, final BitSet outboundChanges) {
        inboundChanges.forEach(
                inboundChange -> {
                    if (inboundChange < inboundFieldReferences.length) {
                        final BitSet outboundFieldIds = inboundFieldReferences[inboundChange];
                        if (outboundFieldIds != null) {
                            outboundChanges.or(outboundFieldIds);
                        }
                    }
                });
    }

    void mapInboundFieldIdToOutboundFieldId(final int inFieldId, final int outFieldId) {
        getOrCreateBitSet(inFieldId).set(outFieldId);
        outboundToInboundMap = IntArray.ensureEntry(outboundToInboundMap, outFieldId, -1);
        outboundToInboundMap[outFieldId] = inFieldId;
    }

    private BitSet getOrCreateBitSet(final int inboundFieldId) {
        inboundFieldReferences = GenericArray.ensureEntry(inboundFieldReferences, inboundFieldId);
        if (inboundFieldReferences[inboundFieldId] == null) {
            inboundFieldReferences[inboundFieldId] = new BitSet();
        }
        return inboundFieldReferences[inboundFieldId];
    }

    private class Resolver implements FieldResolver {
        private int outboundFieldId = -1;
        private Schema inboundSchema;
        private Schema outboundSchema;

        private void reset(final Schema inboundSchema, final Schema outboundSchema) {
            this.inboundSchema = inboundSchema;
            this.outboundSchema = outboundSchema;
            this.outboundFieldId = -1;
        }

        @Override
        public Field findField(final String name) {
            SchemaField depField = outboundSchema.maybeField(name);
            if (depField == null) {
                depField = inboundSchema.maybeField(name);
                if (depField == null) {
                    throw fieldNotFound("");
                }
                getOrCreateBitSet(depField.fieldId()).set(outboundFieldId);
            } else if (depField.fieldId() < outboundToInboundMap.length) {
                final int inboundFieldId = outboundToInboundMap[depField.fieldId()];
                if (inboundFieldId != -1) {
                    getOrCreateBitSet(inboundFieldId).set(outboundFieldId);
                }
            }
            return depField.field();
        }
    }

    // VisibleForTesting
    int referenceCount() {
        int count = 0;
        for (BitSet set : inboundFieldReferences) {
            if (set != null) {
                count += set.cardinality();
            }
        }
        return count;
    }
}
