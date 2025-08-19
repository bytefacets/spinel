// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.join;

import static com.bytefacets.spinel.join.JoinSchemaBuilder.schemaResources;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntConsumer;
import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.spinel.TransformInput;
import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.common.OutputManager;
import com.bytefacets.spinel.schema.ChangedFieldSet;
import com.bytefacets.spinel.schema.FieldMapping;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.transform.OutputProvider;
import java.util.BitSet;
import javax.annotation.Nullable;

public final class Join implements OutputProvider {
    private final JoinChangeTracker changeTracker;
    private final JoinSchemaBuilder schemaBuilder;
    private final LeftInput leftInput;
    private final RightInput rightInput;
    private final OutputManager manager;
    private final TransformOutput output;
    private Schema leftSchema;
    private Schema rightSchema;

    Join(
            final JoinSchemaBuilder schemaBuilder,
            final JoinChangeTracker changeTracker,
            final JoinMapper mapper) {
        this.schemaBuilder = requireNonNull(schemaBuilder, "schemaBuilder");
        this.changeTracker = requireNonNull(changeTracker, "changeTracker");
        this.rightInput = new RightInput(mapper);
        this.leftInput = new LeftInput(mapper);
        this.manager = OutputManager.outputManager(mapper.rowProvider());
        this.output = manager.output();
    }

    public TransformInput leftInput() {
        return leftInput;
    }

    public TransformInput rightInput() {
        return rightInput;
    }

    @Override
    public TransformOutput output() {
        return output;
    }

    private boolean haveBothSchemas() {
        return leftSchema != null && rightSchema != null;
    }

    private void bindSchemaAndUpdate() {
        final var left = schemaResources(leftSchema, true);
        final var right = schemaResources(rightSchema, false);
        final Schema out = schemaBuilder.buildSchema(left, right);
        leftInput.fieldMapping = left.buildFieldMapping();
        leftInput.joinKeyDependencies = left.joinKeyDependencies();
        rightInput.fieldMapping = right.buildFieldMapping();
        rightInput.joinKeyDependencies = right.joinKeyDependencies();
        changeTracker.outboundFieldIds(left.outFieldIds(), right.outFieldIds());
        manager.updateSchema(out);
    }

    private void maybeTearDownSchema() {
        if (manager.schema() != null) {
            manager.updateSchema(null);
        }
        leftInput.mapper.clear();
        schemaBuilder.unbindSchemas();
    }

    private final class LeftInput implements TransformInput {
        private final JoinMapper mapper;
        private FieldMapping fieldMapping;
        private BitSet joinKeyDependencies;
        private TransformOutput source;

        private LeftInput(final JoinMapper mapper) {
            this.mapper = requireNonNull(mapper, "mapper");
        }

        @Override
        public void setSource(@Nullable final TransformOutput output) {
            this.source = output;
        }

        @Override
        public void schemaUpdated(@Nullable final Schema schema) {
            leftSchema = schema;
            if (haveBothSchemas()) {
                bindSchemaAndUpdate();
                // catch up right side; left side will come in with the attachment
                rightInput.addAllSourceRowsIfNecessary();
            } else {
                maybeTearDownSchema();
            }
        }

        @Override
        public void rowsAdded(final IntIterable rows) {
            if (!haveBothSchemas()) {
                return; // outbound not ready
            }
            rows.forEach(mapper::leftRowAdd);
            changeTracker.fire(manager, mapper::cleanUpRemovedRow);
        }

        @Override
        public void rowsChanged(final IntIterable rows, final ChangedFieldSet changedFields) {
            if (!haveBothSchemas()) {
                return; // outbound not ready
            }
            final boolean reEvalKey = changedFields.intersects(joinKeyDependencies);
            fieldMapping.translateInboundChangeSet(changedFields, changeTracker::changeField);
            rows.forEach(row -> mapper.leftRowChange(row, reEvalKey));
            changeTracker.fire(manager, mapper::cleanUpRemovedRow);
        }

        @Override
        public void rowsRemoved(final IntIterable rows) {
            if (!haveBothSchemas()) {
                return;
            }
            rows.forEach(mapper::leftRowRemove);
            changeTracker.fire(manager, mapper::cleanUpRemovedRow);
        }

        private void addAllSourceRowsIfNecessary() {
            source.rowProvider().forEach(mapper::leftRowAdd);
        }
    }

    private final class RightInput implements TransformInput {
        private final JoinMapper mapper;
        private FieldMapping fieldMapping;
        private BitSet joinKeyDependencies;
        private TransformOutput source;

        RightInput(final JoinMapper mapper) {
            this.mapper = requireNonNull(mapper, "mapper");
        }

        @Override
        public void setSource(@Nullable final TransformOutput output) {
            this.source = output;
        }

        @Override
        public void schemaUpdated(@Nullable final Schema schema) {
            rightSchema = schema;
            if (haveBothSchemas()) {
                bindSchemaAndUpdate();
                // catch up left side; right side will come in with the attachment
                leftInput.addAllSourceRowsIfNecessary();
            } else {
                maybeTearDownSchema();
            }
        }

        @Override
        public void rowsAdded(final IntIterable rows) {
            if (!haveBothSchemas()) {
                return; // outbound not ready
            }
            rows.forEach(mapper::rightRowAdd);
            changeTracker.fire(manager, mapper::cleanUpRemovedRow);
        }

        @Override
        public void rowsChanged(final IntIterable rows, final ChangedFieldSet changedFields) {
            if (!haveBothSchemas()) {
                return; // outbound not ready
            }
            final boolean reEvalKey = changedFields.intersects(joinKeyDependencies);
            fieldMapping.translateInboundChangeSet(changedFields, changeTracker::changeField);
            final IntConsumer consumer =
                    reEvalKey ? this::rowChangeWithReEval : this::rowChangeWithNoReEval;
            rows.forEach(consumer);
            changeTracker.fire(manager, mapper::cleanUpRemovedRow);
        }

        private void rowChangeWithNoReEval(final int row) {
            mapper.rightRowChange(row, false);
        }

        private void rowChangeWithReEval(final int row) {
            mapper.rightRowChange(row, true);
        }

        @Override
        public void rowsRemoved(final IntIterable rows) {
            if (!haveBothSchemas()) {
                return; // outbound not ready
            }
            rows.forEach(mapper::rightRowRemove);
            changeTracker.fire(manager, mapper::cleanUpRemovedRow);
        }

        private void addAllSourceRowsIfNecessary() {
            source.rowProvider().forEach(mapper::rightRowAdd);
        }
    }
}
