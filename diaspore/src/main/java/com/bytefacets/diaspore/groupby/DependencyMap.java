// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.groupby;

import static com.bytefacets.diaspore.exception.FieldNotFoundException.fieldNotFound;

import com.bytefacets.collections.arrays.GenericArray;
import com.bytefacets.collections.functional.GenericIterator;
import com.bytefacets.collections.hash.GenericIndexedSet;
import com.bytefacets.collections.queue.GenericHydraDeque;
import com.bytefacets.diaspore.schema.ChangedFieldSet;
import com.bytefacets.diaspore.schema.Field;
import com.bytefacets.diaspore.schema.FieldBitSet;
import com.bytefacets.diaspore.schema.FieldResolver;
import com.bytefacets.diaspore.schema.Schema;
import com.bytefacets.diaspore.schema.SchemaField;
import java.util.BitSet;

class DependencyMap {
    private final GenericHydraDeque<AggregationFunction> inboundTriggers =
            new GenericHydraDeque<>(8, 8);
    private final BitSet outboundFieldChanges = new BitSet();
    private final FieldBitSet outboundFieldSet = FieldBitSet.fieldBitSet(outboundFieldChanges);
    private BitSet[] inboundFieldReferences = GenericArray.create(BitSet.class, 8);
    private final Resolver resolver = new Resolver();
    private final GenericIterator<AggregationFunction> triggerIterator =
            inboundTriggers.iterator(0);
    private final GenericIndexedSet<AggregationFunction> changedFunctions;
    private int groupFieldId = -1;
    private int groupCountFieldId = -1;

    DependencyMap(final int aggFunctionCount) {
        changedFunctions = new GenericIndexedSet<>(Math.max(1, aggFunctionCount), 1f);
    }

    void reset() {
        groupFieldId = -1;
        groupCountFieldId = -1;
        resolver.reset(null, null);
        for (BitSet set : inboundFieldReferences) {
            if (set != null) {
                set.clear();
            }
        }
    }

    FieldBitSet outboundFieldChangeSet() {
        return outboundFieldSet;
    }

    int groupFieldId() {
        return groupFieldId;
    }

    FieldResolver resolver(final Schema inSchema, final AggregationFunction function) {
        resolver.reset(inSchema, function);
        return resolver;
    }

    GenericIndexedSet<AggregationFunction> translateInboundChangeFields(
            final ChangedFieldSet inboundChanges) {
        changedFunctions.clear();
        inboundChanges.forEach(
                inboundChange -> {
                    if (inboundChange < inboundFieldReferences.length) {
                        final BitSet outboundFieldIds = inboundFieldReferences[inboundChange];
                        if (outboundFieldIds != null) {
                            outboundFieldChanges.or(outboundFieldIds);
                        }
                    }
                    inboundTriggers
                            .iterator(inboundChange, triggerIterator)
                            .forEach(changedFunctions::add);
                });
        return changedFunctions;
    }

    void mapInboundFieldIdToOutboundFieldId(final int inFieldId, final int outFieldId) {
        getOrCreateBitSet(inFieldId).set(outFieldId);
    }

    private BitSet getOrCreateBitSet(final int inboundFieldId) {
        inboundFieldReferences = GenericArray.ensureEntry(inboundFieldReferences, inboundFieldId);
        if (inboundFieldReferences[inboundFieldId] == null) {
            inboundFieldReferences[inboundFieldId] = new BitSet();
        }
        return inboundFieldReferences[inboundFieldId];
    }

    void setGroupIdFieldId(final int groupFieldId) {
        this.groupFieldId = groupFieldId;
    }

    void setGroupCountFieldId(final int countFieldId) {
        this.groupCountFieldId = countFieldId;
    }

    void markCountChanged() {
        if (groupCountFieldId != -1) {
            outboundFieldChanges.set(groupCountFieldId);
        }
    }

    private class Resolver implements FieldResolver {
        private AggregationFunction function;
        private Schema inboundSchema;
        private final BitSet inboundIds = new BitSet();

        private void reset(final Schema inboundSchema, final AggregationFunction function) {
            this.inboundSchema = inboundSchema;
            this.function = function;
            this.inboundIds.clear();
        }

        @Override
        public Field findField(final String name) {
            final SchemaField depField = inboundSchema.maybeField(name);
            if (depField == null) {
                throw fieldNotFound(
                        name,
                        String.format(
                                "aggregate function '%s'", function.getClass().getSimpleName()),
                        inboundSchema.name());
            }
            final int inboundId = depField.fieldId();
            if (!inboundIds.get(inboundId)) {
                inboundTriggers.addLast(inboundId, function);
                inboundIds.set(inboundId);
            }
            return depField.field();
        }
    }
}
