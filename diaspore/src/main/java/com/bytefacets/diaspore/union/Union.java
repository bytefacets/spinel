// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.union;

import static com.bytefacets.diaspore.common.OutputManager.outputManager;
import static com.bytefacets.diaspore.exception.OperatorSetupException.setupException;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.collections.hash.StringGenericIndexedMap;
import com.bytefacets.diaspore.TransformInput;
import com.bytefacets.diaspore.TransformOutput;
import com.bytefacets.diaspore.common.OutputManager;
import com.bytefacets.diaspore.common.StateChange;
import com.bytefacets.diaspore.schema.ChangedFieldSet;
import com.bytefacets.diaspore.schema.Schema;
import com.bytefacets.diaspore.transform.OutputProvider;
import javax.annotation.Nullable;

public final class Union implements OutputProvider {
    private final StateChange stateChange = StateChange.stateChange();
    private final UnionSchemaBuilder schemaBuilder;
    private final UnionRowMapper mapper;
    private final OutputManager outputManager;
    private final DependencyMap dependencyMap;
    private final StringGenericIndexedMap<Input> activeInputs = new StringGenericIndexedMap<>(4);
    private Schema outSchema;

    Union(final int initialSize, final UnionSchemaBuilder schemaBuilder) {
        this.mapper = new UnionRowMapper(initialSize);
        this.schemaBuilder = requireNonNull(schemaBuilder, "schemaBuilder");
        this.outputManager = outputManager(mapper.asRowProvider());
        this.dependencyMap = schemaBuilder.dependencyMap();
    }

    @Override
    public TransformOutput output() {
        return outputManager.output();
    }

    public TransformInput newInput(final String name) {
        if (activeInputs.containsKey(name)) {
            throw setupException("You must use unique names when requesting new inputs: " + name);
        }
        final int inputIndex = activeInputs.add(name);
        final Input input = new Input(inputIndex);
        activeInputs.putValueAt(inputIndex, input);
        mapper.mapInputName(inputIndex, name);
        return input;
    }

    public TransformInput input(final String name) {
        return activeInputs.getOrDefault(name, null);
    }

    private void inputDeactivated(final Input input) {
        activeInputs.removeAt(input.inputIndex);
    }

    private void mapSchema(final int inputId, final Schema schema) {
        if (outSchema == null) {
            outSchema = schemaBuilder.buildSchema(inputId, schema, mapper);
            outputManager.updateSchema(outSchema);
        } else {
            schemaBuilder.mapNewSource(outSchema, inputId, schema);
        }
    }

    private void unmapSchema(final Input input) {
        if (outSchema != null) {
            schemaBuilder.unmapInput(input.inputIndex, outSchema);
            input.removeFromOutput();
            // REVISIT: do we want to tear down outSchema when all inputs are null?
        }
    }

    private final class Input implements TransformInput {
        private final int inputIndex;
        private TransformOutput source;

        private Input(final int inputIndex) {
            this.inputIndex = inputIndex;
        }

        private void removeFromOutput() {
            mapper.iterateInputRowsInOutput(inputIndex, stateChange::removeRow);
            stateChange.fire(outputManager, mapper::removeRowOnInputRemoval);
        }

        @Override
        public void setSource(@Nullable final TransformOutput output) {
            final boolean disconnected = this.source != null && output == null;
            this.source = output;
            if (disconnected) {
                inputDeactivated(this);
            }
        }

        @Override
        public void schemaUpdated(@Nullable final Schema schema) {
            if (schema != null) {
                mapSchema(inputIndex, schema);
            } else {
                unmapSchema(this);
            }
        }

        @Override
        public void rowsAdded(final IntIterable rows) {
            rows.forEach(
                    row -> {
                        final int outRow = mapper.mapInputRow(inputIndex, row);
                        stateChange.addRow(outRow);
                    });
            stateChange.fire(outputManager, null);
        }

        @Override
        public void rowsChanged(final IntIterable rows, final ChangedFieldSet changedFields) {
            dependencyMap.translateChanges(inputIndex, changedFields, stateChange::changeField);
            rows.forEach(
                    row -> {
                        final int outRow = mapper.lookupOutboundRow(inputIndex, row);
                        stateChange.changeRow(outRow);
                    });
            stateChange.fire(outputManager, null);
        }

        @Override
        public void rowsRemoved(final IntIterable rows) {
            rows.forEach(
                    row -> {
                        final int outRow = mapper.removeInputRow(inputIndex, row);
                        stateChange.removeRow(outRow);
                    });
            stateChange.fire(outputManager, mapper::freeOutRow);
        }
    }
}
