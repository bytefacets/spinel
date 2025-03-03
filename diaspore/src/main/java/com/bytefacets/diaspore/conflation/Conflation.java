// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.conflation;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntConsumer;
import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.collections.hash.IntIndexedSet;
import com.bytefacets.collections.vector.IntVector;
import com.bytefacets.diaspore.TransformInput;
import com.bytefacets.diaspore.TransformOutput;
import com.bytefacets.diaspore.common.OutputManager;
import com.bytefacets.diaspore.schema.ChangedFieldSet;
import com.bytefacets.diaspore.schema.FieldBitSet;
import com.bytefacets.diaspore.schema.FieldMapping;
import com.bytefacets.diaspore.schema.Schema;
import javax.annotation.Nullable;

public final class Conflation {
    private final OutputManager outputManager;
    private final Input input;
    private final TransformOutput output;
    private final ConflationSchemaBuilder schemaBuilder;

    Conflation(
            final ConflationSchemaBuilder schemaBuilder,
            final int initialCapacity,
            final int maxPendingRows) {
        this.schemaBuilder = requireNonNull(schemaBuilder, "schemaBuilder");
        this.input = new Input(initialCapacity, maxPendingRows);
        this.outputManager = OutputManager.outputManager(x -> {});
        this.output = outputManager.output();
    }

    public TransformInput input() {
        return input;
    }

    public TransformOutput output() {
        return output;
    }

    public void firePendingChanges() {
        input.pending.fire();
    }

    public int changesPending() {
        return input.pending.pendingRows.size();
    }

    private final class Input implements TransformInput {
        private final Pending pending;
        private final FieldBitSet fieldSet;

        private Input(final int initialCapacity, final int maxPendingRows) {
            this.pending = new Pending(initialCapacity, maxPendingRows);
            this.fieldSet = FieldBitSet.fieldBitSet();
        }

        @Override
        public void schemaUpdated(@Nullable final Schema schema) {
            if (schema != null) {
                final var fieldMappingBuilder = FieldMapping.fieldMapping(schema.size());
                final var outSchema = schemaBuilder.buildSchema(schema, fieldMappingBuilder);
                pending.fieldMapping = fieldMappingBuilder.build();
                outputManager.updateSchema(outSchema);
            } else {
                outputManager.updateSchema(null);
            }
        }

        @Override
        public void rowsAdded(final IntIterable rows) {
            outputManager.notifyAdds(rows);
        }

        @Override
        public void rowsChanged(final IntIterable rows, final ChangedFieldSet changedFields) {
            pending.addAllToBatch(rows, changedFields);
        }

        @Override
        public void rowsRemoved(final IntIterable rows) {
            if (pending.captureRemoved(rows)) {
                pending.fire();
                outputManager.notifyRemoves(rows);
            }
        }

        private final class Pending implements IntIterable {
            private final int batchSize;
            private final IntIndexedSet pendingRowsSet;
            private final IntIndexedSet tmpRemovedRows;
            private final IntVector pendingRows;
            private FieldMapping fieldMapping;
            private ChangedFieldSet inboundChangedFields;
            private boolean fieldChangesNeedApplying;

            private Pending(final int initialCapacity, final int batchSize) {
                this.batchSize = batchSize;
                this.pendingRows = new IntVector(batchSize);
                this.pendingRowsSet = new IntIndexedSet(initialCapacity);
                this.tmpRemovedRows = new IntIndexedSet(16);
            }

            private void fire() {
                outputManager.notifyChanges(this, fieldSet);
                fieldSet.clear();
                forEach(pendingRowsSet::remove);
                pending.tmpRemovedRows.clear();
                pendingRows.clear();
                fieldChangesNeedApplying = true;
            }

            private void applyInboundFieldChangesIfNecessary() {
                if (fieldChangesNeedApplying && inboundChangedFields != null) {
                    fieldMapping.translateInboundChangeSet(
                            inboundChangedFields, fieldSet::fieldChanged);
                    fieldChangesNeedApplying = false;
                }
            }

            private void addAllToBatch(
                    final IntIterable rows, final ChangedFieldSet inboundChangedFields) {
                this.inboundChangedFields = inboundChangedFields;
                this.fieldChangesNeedApplying = true;
                rows.forEach(this::addToBatch);
                this.inboundChangedFields = null;
                this.fieldChangesNeedApplying = false;
            }

            private void addToBatch(final int row) {
                final int sizeBefore = pendingRowsSet.size();
                pendingRowsSet.add(row);
                if (pendingRowsSet.size() != sizeBefore) {
                    if (pendingRowsSet.size() == batchSize) {
                        fire();
                    }
                    applyInboundFieldChangesIfNecessary();
                    pendingRows.append(row);
                }
            }

            @Override
            public void forEach(final IntConsumer action) {
                final boolean checkRemoved = !tmpRemovedRows.isEmpty();
                for (int i = 0, len = pendingRows.size(); i < len; i++) {
                    final int row = pendingRows.valueAt(i);
                    if (!checkRemoved || !tmpRemovedRows.containsKey(row)) {
                        action.accept(row);
                    }
                }
            }

            private boolean captureRemoved(final IntIterable rows) {
                rows.forEach(tmpRemovedRows::add);
                return !tmpRemovedRows.isEmpty();
            }
        }
    }
}
