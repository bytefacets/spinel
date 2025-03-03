// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.join;

import static com.bytefacets.diaspore.join.JoinSchemaBuilder.schemaResources;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.diaspore.TransformInput;
import com.bytefacets.diaspore.TransformOutput;
import com.bytefacets.diaspore.common.OutputManager;
import com.bytefacets.diaspore.schema.ChangedFieldSet;
import com.bytefacets.diaspore.schema.FieldMapping;
import com.bytefacets.diaspore.schema.Schema;
import com.bytefacets.diaspore.transform.OutputProvider;
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

    private void schemasUpdates() {
        final boolean ready = leftSchema != null && rightSchema != null;
        if (ready) {
            bindSchemaAndUpdate();
        } else {
            maybeTearDownSchema();
        }
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
        schemaBuilder.unbindSchemas();
    }

    private final class LeftInput implements TransformInput {
        private final JoinMapper mapper;
        private FieldMapping fieldMapping;
        private BitSet joinKeyDependencies;

        private LeftInput(final JoinMapper mapper) {
            this.mapper = requireNonNull(mapper, "mapper");
        }

        @Override
        public void schemaUpdated(@Nullable final Schema schema) {
            leftSchema = schema;
        }

        @Override
        public void rowsAdded(final IntIterable rows) {
            rows.forEach(mapper::leftRowAdd);
            changeTracker.fire(manager, mapper::cleanUpRemovedRow);
        }

        @Override
        public void rowsChanged(final IntIterable rows, final ChangedFieldSet changedFields) {
            final boolean reEvalKey = changedFields.intersects(joinKeyDependencies);
            fieldMapping.translateInboundChangeSet(changedFields, changeTracker::changeField);
            rows.forEach(row -> mapper.leftRowChange(row, reEvalKey));
            changeTracker.fire(manager, mapper::cleanUpRemovedRow);
        }

        @Override
        public void rowsRemoved(final IntIterable rows) {
            rows.forEach(mapper::leftRowRemove);
            changeTracker.fire(manager, mapper::cleanUpRemovedRow);
        }
    }

    private final class RightInput implements TransformInput {
        private final JoinMapper mapper;
        private FieldMapping fieldMapping;
        private BitSet joinKeyDependencies;

        RightInput(final JoinMapper mapper) {
            this.mapper = requireNonNull(mapper, "mapper");
        }

        @Override
        public void schemaUpdated(@Nullable final Schema schema) {
            rightSchema = schema;
            schemasUpdates();
        }

        @Override
        public void rowsAdded(final IntIterable rows) {
            rows.forEach(mapper::rightRowAdd);
            changeTracker.fire(manager, mapper::cleanUpRemovedRow);
        }

        @Override
        public void rowsChanged(final IntIterable rows, final ChangedFieldSet changedFields) {
            final boolean reEvalKey = changedFields.intersects(joinKeyDependencies);
            fieldMapping.translateInboundChangeSet(changedFields, changeTracker::changeField);
            rows.forEach(row -> mapper.rightRowChange(row, reEvalKey));
            changeTracker.fire(manager, mapper::cleanUpRemovedRow);
        }

        @Override
        public void rowsRemoved(final IntIterable rows) {
            rows.forEach(mapper::rightRowRemove);
            changeTracker.fire(manager, mapper::cleanUpRemovedRow);
        }
    }
}
