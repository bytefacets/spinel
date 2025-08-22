// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.projection;

import static com.bytefacets.spinel.common.DelegatedRowProvider.delegatedRowProvider;
import static com.bytefacets.spinel.common.OutputManager.outputManager;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.spinel.TransformInput;
import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.common.OutputManager;
import com.bytefacets.spinel.common.StateChange;
import com.bytefacets.spinel.schema.ChangedFieldSet;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.transform.InputProvider;
import com.bytefacets.spinel.transform.OutputProvider;
import java.util.BitSet;
import jakarta.annotation.Nullable;

public final class Projection implements InputProvider, OutputProvider {
    private final ProjectionSchemaBuilder schemaBuilder;
    private final OutputManager outputManager;
    private final Input input = new Input();

    Projection(final ProjectionSchemaBuilder schemaBuilder) {
        this.schemaBuilder = requireNonNull(schemaBuilder, "schemaBuilder");
        this.outputManager = outputManager(delegatedRowProvider(() -> input.source));
    }

    @Override
    public TransformOutput output() {
        return outputManager.output();
    }

    @Override
    public TransformInput input() {
        return input;
    }

    private class Input implements TransformInput {
        private final BitSet outChanges = new BitSet();
        private final StateChange changes = StateChange.stateChange(outChanges);
        private TransformOutput source;
        private final ProjectionDependencyMap dependencyMap = new ProjectionDependencyMap();

        @Override
        public void setSource(@Nullable final TransformOutput output) {
            this.source = output;
        }

        @Override
        public void schemaUpdated(@Nullable final Schema schema) {
            dependencyMap.reset();
            if (schema != null) {
                final Schema outboundSchema =
                        schemaBuilder.buildOutboundSchema(schema, dependencyMap);
                outputManager.updateSchema(outboundSchema);
            } else {
                outputManager.updateSchema(null);
            }
        }

        @Override
        public void rowsAdded(final IntIterable rows) {
            rows.forEach(changes::addRow);
            changes.fire(outputManager, null);
        }

        @Override
        public void rowsChanged(final IntIterable rows, final ChangedFieldSet changedFields) {
            outChanges.clear();
            dependencyMap.translateInboundChangeFields(changedFields, outChanges);
            if (outChanges.nextSetBit(0) >= 0) {
                rows.forEach(changes::changeRow);
                changes.fire(outputManager, null);
            }
        }

        @Override
        public void rowsRemoved(final IntIterable rows) {
            rows.forEach(changes::removeRow);
            changes.fire(outputManager, null);
        }
    }
}
