// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.projection;

import static com.bytefacets.diaspore.common.DelegatedRowProvider.delegatedRowProvider;
import static com.bytefacets.diaspore.common.OutputManager.outputManager;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.diaspore.TransformInput;
import com.bytefacets.diaspore.TransformOutput;
import com.bytefacets.diaspore.common.OutputManager;
import com.bytefacets.diaspore.common.StateChange;
import com.bytefacets.diaspore.schema.ChangedFieldSet;
import com.bytefacets.diaspore.schema.Schema;
import java.util.BitSet;
import javax.annotation.Nullable;

public final class Projection {
    private final ProjectionSchemaBuilder schemaBuilder;
    private final OutputManager outputManager;
    private final Input input = new Input();

    Projection(final ProjectionSchemaBuilder schemaBuilder) {
        this.schemaBuilder = requireNonNull(schemaBuilder, "schemaBuilder");
        this.outputManager = outputManager(delegatedRowProvider(() -> input.source));
    }

    public TransformOutput output() {
        return outputManager.output();
    }

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
