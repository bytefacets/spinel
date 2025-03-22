// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.filter;

import static com.bytefacets.diaspore.common.OutputManager.outputManager;
import static com.bytefacets.diaspore.schema.MappedFieldFactory.asMappedField;
import static com.bytefacets.diaspore.schema.SchemaFieldResolver.schemaFieldResolver;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.collections.hash.IntIndexedSet;
import com.bytefacets.diaspore.TransformInput;
import com.bytefacets.diaspore.TransformOutput;
import com.bytefacets.diaspore.common.OutputManager;
import com.bytefacets.diaspore.common.StateChange;
import com.bytefacets.diaspore.schema.ChangedFieldSet;
import com.bytefacets.diaspore.schema.FieldMapping;
import com.bytefacets.diaspore.schema.Schema;
import com.bytefacets.diaspore.schema.SchemaBuilder;
import com.bytefacets.diaspore.schema.SchemaFieldResolver;
import java.util.BitSet;
import javax.annotation.Nullable;

public final class Filter {
    private final OutputManager outputManager;
    private final Input input;
    private final String name;

    Filter(final String name, final int initialSize, final RowPredicate defaultPredicate) {
        this.name = requireNonNull(name, "name");
        this.input = new Input(initialSize, defaultPredicate);
        this.outputManager = outputManager(input.passingRows::forEachEntry);
    }

    public void updatePredicate(@Nullable final RowPredicate predicate) {
        final RowPredicate newPredicate = requireNonNullElse(predicate, input.defaultPredicate);
        input.updatePredicate(newPredicate);
    }

    public TransformInput input() {
        return input;
    }

    public TransformOutput output() {
        return outputManager.output();
    }

    private final class Input implements TransformInput {
        private final RowPredicate defaultPredicate;
        private final IntIndexedSet passingRows;
        private final StateChange stateChange;
        private final BitSet fieldDependencies = new BitSet();
        private final SchemaFieldResolver fieldResolver =
                schemaFieldResolver(fieldDependencies::set);
        private FieldMapping fieldMapping;
        private Schema inboundSchema;
        private TransformOutput source;
        private RowPredicate predicate;

        private Input(final int initialSize, final RowPredicate defaultPredicate) {
            this.passingRows = new IntIndexedSet(initialSize);
            this.defaultPredicate = requireNonNull(defaultPredicate, "defaultPredicate");
            this.stateChange = StateChange.stateChange();
            this.predicate = defaultPredicate;
        }

        private void bindPredicate() {
            if (inboundSchema != null) {
                fieldDependencies.clear();
                fieldResolver.setSchema(inboundSchema);
                predicate.bindToSchema(fieldResolver);
            }
        }

        private void updatePredicate(final RowPredicate newPredicate) {
            if (this.predicate != newPredicate) {
                this.predicate.unbindSchema();
                this.predicate = newPredicate;
                bindPredicate();
                if (source != null) {
                    source.rowProvider().forEach(this::retestPredicateChange);
                    fire();
                }
            }
        }

        @Override
        public void setSource(final TransformOutput output) {
            this.source = output;
        }

        @Override
        public void schemaUpdated(final Schema schema) {
            this.inboundSchema = schema;
            if (schema != null) {
                bindPredicate();
                buildOutboundSchema();
            } else {
                fieldResolver.setSchema(null);
                predicate.unbindSchema();
                outputManager.updateSchema(null);
            }
        }

        private int sourceRowOf(final int outboundRow) {
            return passingRows.getKeyAt(outboundRow);
        }

        private void buildOutboundSchema() {
            final var sb = SchemaBuilder.schemaBuilder(name, inboundSchema.size());
            sb.addInboundSchema(
                    inboundSchema,
                    (inboundField, outboundFieldId) ->
                            asMappedField(inboundField.field(), this::sourceRowOf));
            fieldMapping = sb.buildFieldMapping();
            outputManager.updateSchema(sb.buildSchema());
        }

        @Override
        public void rowsAdded(final IntIterable rows) {
            rows.forEach(
                    inRow -> {
                        if (predicate.testRow(inRow)) {
                            final int outboundRow = passingRows.add(inRow);
                            stateChange.addRow(outboundRow);
                        }
                    });
            fire();
        }

        @Override
        public void rowsChanged(final IntIterable rows, final ChangedFieldSet changedFields) {
            fieldMapping.translateInboundChangeSet(changedFields, stateChange::changeField);
            final boolean retest = changedFields.intersects(fieldDependencies);
            if (retest) {
                rows.forEach(this::retestChange);
            } else {
                rows.forEach(this::forwardChangeIfNecessary);
            }
            fire();
        }

        @Override
        public void rowsRemoved(final IntIterable rows) {
            rows.forEach(this::forwardRemoveIfNecessary);
            fire();
        }

        private void forwardRemoveIfNecessary(final int inRow) {
            final int outbound = passingRows.lookupEntry(inRow);
            if (outbound != -1) {
                passingRows.removeAtAndReserve(outbound);
                stateChange.removeRow(outbound);
            }
        }

        private void forwardChangeIfNecessary(final int inRow) {
            final int outboundRow = passingRows.lookupEntry(inRow);
            if (outboundRow != -1) {
                stateChange.changeRow(outboundRow);
            }
        }

        private void retestPredicateChange(final int inRow) {
            if (predicate.testRow(inRow)) {
                final int sizeBefore = passingRows.size();
                final int outboundRow = passingRows.add(inRow);
                if (sizeBefore != passingRows.size()) {
                    stateChange.addRow(outboundRow);
                }
            } else {
                final int outboundRow = passingRows.lookupEntry(inRow);
                if (outboundRow != -1) {
                    passingRows.removeAtAndReserve(outboundRow);
                    stateChange.removeRow(outboundRow);
                }
            }
        }

        private void retestChange(final int inRow) {
            int outboundRow = passingRows.lookupEntry(inRow);
            final boolean wasPassingBefore = outboundRow != -1;
            if (predicate.testRow(inRow)) {
                if (wasPassingBefore) {
                    stateChange.changeRow(outboundRow);
                } else {
                    outboundRow = passingRows.add(inRow);
                    stateChange.addRow(outboundRow);
                }
            } else if (wasPassingBefore) {
                stateChange.removeRow(outboundRow);
            }
        }

        private void fire() {
            stateChange.fire(outputManager, passingRows::freeReservedEntry);
        }
    }
}
