// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.groupby;

import static com.bytefacets.spinel.common.DelegatedRowProvider.delegatedRowProvider;
import static com.bytefacets.spinel.common.OutputManager.outputManager;
import static com.bytefacets.spinel.common.StateChangeSet.stateChangeSet;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.collections.hash.GenericIndexedSet;
import com.bytefacets.spinel.TransformInput;
import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.cache.Cache;
import com.bytefacets.spinel.common.BitSetRowProvider;
import com.bytefacets.spinel.common.OutputManager;
import com.bytefacets.spinel.common.StateChangeSet;
import com.bytefacets.spinel.schema.ChangedFieldSet;
import com.bytefacets.spinel.schema.FieldBitSet;
import com.bytefacets.spinel.schema.FieldMapping;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.transform.InputProvider;
import com.bytefacets.spinel.transform.OutputProvider;
import java.util.BitSet;
import java.util.Collection;
import javax.annotation.Nullable;

public final class GroupBy implements InputProvider, OutputProvider {
    private final GroupMapping groupMapping;
    private final GroupBySchemaBuilder schemaBuilder;
    private final ChildSchemaBuilder childSchemaBuilder;
    private final OutputManager parentOutput;
    private final OutputManager childOutput;
    private final Input input;
    private final GroupFunction groupFunction;

    GroupBy(
            final GroupBySchemaBuilder schemaBuilder,
            final ChildSchemaBuilder childSchemaBuilder,
            final GroupFunction groupFunction,
            final int initialOutboundSize,
            final int initialInboundSize) {
        this.schemaBuilder = requireNonNull(schemaBuilder, "schemaBuilder");
        this.childSchemaBuilder = requireNonNull(childSchemaBuilder, "childSchemaBuilder");
        this.groupMapping = new GroupMapping(initialOutboundSize, initialInboundSize);
        this.input = new Input(schemaBuilder.aggregationFunctions());
        this.parentOutput = outputManager(input.parentRowProvider);
        this.childOutput = outputManager(delegatedRowProvider(() -> input.source));
        this.groupFunction = requireNonNull(groupFunction, "groupFunction");
    }

    @Override
    public TransformInput input() {
        return input;
    }

    /** The parent output */
    @Override
    public TransformOutput output() {
        return parentOutput.output();
    }

    public TransformOutput parentOutput() {
        return parentOutput.output();
    }

    public TransformOutput childOutput() {
        return childOutput.output();
    }

    private final class Input implements TransformInput {
        private final GroupFunctionBinding groupFunctionBinding = new GroupFunctionBinding();
        private final DependencyMap dependencyMap;
        private final FieldBitSet fieldBitSet;
        private final StateChangeSet stateChange;
        private final Collection<AggregationFunction> aggregationFunctions;
        private final GroupRowMods rowsChangedInGroups;
        private final GroupRowMods rowsAddedToGroups;
        private final GroupRowMods rowsRemovedFromGroups;
        private final FieldBitSet childFieldBitSet = FieldBitSet.fieldBitSet();
        private final BitSet activeGroups = new BitSet();
        private final BitSetRowProvider parentRowProvider =
                BitSetRowProvider.bitSetRowProvider(activeGroups);
        private TransformOutput source;
        private Schema inboundSchema;
        private Cache cache;
        private FieldMapping childFieldMapping;
        private int childGroupFieldId = -1;

        private Input(final Collection<AggregationFunction> aggregationFunctions) {
            this.aggregationFunctions = aggregationFunctions;
            this.dependencyMap = schemaBuilder.dependencyMap();
            this.fieldBitSet = dependencyMap.outboundFieldChangeSet();
            this.stateChange = stateChangeSet(fieldBitSet);
            this.rowsAddedToGroups = new GroupRowMods(16);
            this.rowsChangedInGroups = new GroupRowMods(16);
            this.rowsRemovedFromGroups = new GroupRowMods(16);
        }

        @Override
        public void setSource(@Nullable final TransformOutput output) {
            this.source = output; // UPCOMING: this was passing tests without this line
        }

        @Override
        public void schemaUpdated(@Nullable final Schema schema) {
            this.inboundSchema = schema;
            if (inboundSchema != null) {
                setUp();
            } else {
                tearDown();
            }
        }

        private void setUp() {
            groupMapping.reset();
            groupFunctionBinding.bind(inboundSchema, groupFunction);
            final Schema outSchema = schemaBuilder.buildParentSchema(inboundSchema, groupMapping);
            parentOutput.updateSchema(outSchema);
            cache = schemaBuilder.cache();

            // child output schema
            final var child = childSchemaBuilder.buildChildSchema(inboundSchema, groupMapping);
            childOutput.updateSchema(child.schema());
            childFieldMapping = child.fieldMapping();
            childGroupFieldId = child.groupFieldId();
        }

        private void tearDown() {
            if (cache != null) {
                cache.unbind();
            }
            groupMapping.reset();
            groupFunction.unbindSchema();
            aggregationFunctions.forEach(AggregationFunction::unbindSchema);
            childOutput.updateSchema(null);
            parentOutput.updateSchema(null);
            childFieldMapping = null;
        }

        @Override
        public void rowsAdded(final IntIterable rows) {
            rows.forEach(row -> processRowAddedToGroup(groupFunction.group(row), row));
            updateAllFunctions();
            fire();
            cache.updateAll(rows);
            //
            childOutput.notifyAdds(rows);
        }

        @Override
        public void rowsChanged(final IntIterable rows, final ChangedFieldSet changedFields) {
            fieldBitSet.clear();
            childFieldBitSet.clear();
            final GenericIndexedSet<AggregationFunction> changedFunctions =
                    dependencyMap.translateInboundChangeFields(changedFields);
            if (groupFunctionBinding.isChanged(changedFields)) {
                processChangesForPossibleChangedGroups(rows, changedFunctions);
            } else {
                processChangesForStableGroups(rows, changedFunctions);
            }
            fire();
            cache.updateSelected(rows, changedFields);
            //
            childFieldMapping.translateInboundChangeSet(
                    changedFields, childFieldBitSet::fieldChanged);
            childOutput.notifyChanges(rows, childFieldBitSet);
        }

        private void processChangesForPossibleChangedGroups(
                final IntIterable rows,
                final GenericIndexedSet<AggregationFunction> changedFunctions) {
            rows.forEach(
                    row -> {
                        final int newGroup = groupFunction.group(row);
                        final int oldGroup = groupMapping.groupOfInboundRow(row);
                        if (newGroup == oldGroup) {
                            processGroupUpdateFromChange(newGroup, row);
                        } else {
                            if (childGroupFieldId != -1) {
                                childFieldBitSet.fieldChanged(childGroupFieldId);
                            }
                            processRowRemovedFromGroup(oldGroup, row); // must remove first
                            processRowAddedToGroup(newGroup, row);
                        }
                    });
            if (rowsAddedToGroups.isEmpty() && rowsRemovedFromGroups.isEmpty()) {
                updateChangedFunctions(changedFunctions);
            } else {
                updateAllFunctions();
            }
        }

        private void processChangesForStableGroups(
                final IntIterable rows,
                final GenericIndexedSet<AggregationFunction> changedFunctions) {
            rows.forEach(
                    row -> {
                        final int oldGroup = groupMapping.groupOfInboundRow(row);
                        processGroupUpdateFromChange(oldGroup, row);
                    });
            updateChangedFunctions(changedFunctions);
        }

        @Override
        public void rowsRemoved(final IntIterable rows) {
            rows.forEach(
                    row -> {
                        final int group = groupMapping.groupOfInboundRow(row);
                        processRowRemovedFromGroup(group, row);
                    });
            updateAllFunctions();
            fire();
            cache.updateAll(rows);
            childOutput.notifyRemoves(rows);
        }

        private void processRowAddedToGroup(final int group, final int row) {
            final int oldCount = groupMapping.groupCount(group);
            groupMapping.mapRowToGroup(row, group);
            if (oldCount == 0) {
                stateChange.addRow(group);
                activeGroups.set(group);
            } else {
                dependencyMap.markCountChanged();
                stateChange.changeRowIfNotAdded(group);
            }
            rowsAddedToGroups.addGroupRow(group, row);
        }

        private void processRowRemovedFromGroup(final int group, final int row) {
            groupMapping.unmapRow(row);
            final int newCount = groupMapping.groupCount(group);
            if (newCount == 0) {
                stateChange.removeRow(group);
                activeGroups.clear(group);
            } else {
                dependencyMap.markCountChanged();
                stateChange.changeRow(group);
            }
            rowsRemovedFromGroups.addGroupRow(group, row);
        }

        private void processGroupUpdateFromChange(final int group, final int row) {
            stateChange.changeRowIfNotAdded(group);
            rowsChangedInGroups.addGroupRow(group, row);
        }

        private void updateChangedFunctions(
                final GenericIndexedSet<AggregationFunction> changedFunctions) {
            changedFunctions.forEach(this::updateFunction);
        }

        private void updateAllFunctions() {
            aggregationFunctions.forEach(this::updateFunction);
        }

        private void updateFunction(final AggregationFunction function) {
            rowsAddedToGroups.fire(function::groupRowsAdded);
            rowsChangedInGroups.fire(function::groupRowsChanged);
            rowsRemovedFromGroups.fire(function::groupRowsRemoved);
        }

        private void fire() {
            stateChange.fire(parentOutput, groupFunction::onEmptyGroup);

            rowsAddedToGroups.reset();
            rowsChangedInGroups.reset();
            rowsRemovedFromGroups.reset();
        }
    }
}
