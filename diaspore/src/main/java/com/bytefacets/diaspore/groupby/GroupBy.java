// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.groupby;

import static com.bytefacets.diaspore.common.DelegatedRowProvider.delegatedRowProvider;
import static com.bytefacets.diaspore.common.OutputManager.outputManager;
import static com.bytefacets.diaspore.common.StateChangeSet.stateChangeSet;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.collections.hash.GenericIndexedSet;
import com.bytefacets.diaspore.TransformInput;
import com.bytefacets.diaspore.TransformOutput;
import com.bytefacets.diaspore.RowProvider;
import com.bytefacets.diaspore.cache.Cache;
import com.bytefacets.diaspore.common.OutputManager;
import com.bytefacets.diaspore.common.StateChangeSet;
import com.bytefacets.diaspore.schema.ChangedFieldSet;
import com.bytefacets.diaspore.schema.FieldBitSet;
import com.bytefacets.diaspore.schema.Schema;
import java.util.Collection;
import javax.annotation.Nullable;

public class GroupBy {
    private final GroupMapping groupMapping;
    private final GroupBySchemaBuilder schemaBuilder;
    private final OutputManager parentOutput;
    private final OutputManager childOutput;
    private final Input input;
    private final GroupFunction groupFunction;

    GroupBy(
            final GroupBySchemaBuilder schemaBuilder,
            final GroupFunction groupFunction,
            final int initialOutboundSize,
            final int initialInboundSize) {
        this.schemaBuilder = requireNonNull(schemaBuilder, "schemaBuilder");
        this.groupMapping = new GroupMapping(initialOutboundSize, initialInboundSize);
        this.input = new Input(schemaBuilder.aggregationFunctions());
        this.parentOutput = outputManager(createParentRowProvider());
        this.childOutput = outputManager(delegatedRowProvider(() -> input.source));
        this.groupFunction = requireNonNull(groupFunction, "groupFunction");
    }

    public TransformInput input() {
        return input;
    }

    public TransformOutput parentOutput() {
        return parentOutput.output();
    }

    public TransformOutput childOutput() {
        return childOutput.output();
    }

    private RowProvider createParentRowProvider() {
        return action -> {}; // UPCOMING
    }

    private final class Input implements TransformInput {
        private final GroupFunctionBinding groupFunctionBinding = new GroupFunctionBinding();
        private final DependencyMap dependencyMap;
        private final FieldBitSet fieldBitSet;
        private final StateChangeSet stateChange;
        private final Collection<AggregationFunction> aggregationFunctions;
        private final GenericIndexedSet<AggregationFunction> changedFunctions;
        private final GroupRowMods rowsChangedInGroups;
        private final GroupRowMods rowsAddedToGroups;
        private final GroupRowMods rowsRemovedFromGroups;
        private TransformOutput source;
        private Schema inboundSchema;
        private Cache cache;

        private Input(final Collection<AggregationFunction> aggregationFunctions) {
            this.aggregationFunctions = aggregationFunctions;
            this.changedFunctions = new GenericIndexedSet<>(aggregationFunctions.size(), 1f);
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
            // UPCOMING: child output schema
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
        }

        @Override
        public void rowsAdded(final IntIterable rows) {
            rows.forEach(row -> processRowAddedToGroup(groupFunction.group(row), row));
            updateAllFunctions();
            fire();
            cache.updateAll(rows);
        }

        @Override
        public void rowsChanged(final IntIterable rows, final ChangedFieldSet changedFields) {
            changedFunctions.clear();
            fieldBitSet.clear();
            dependencyMap.translateInboundChangeFields(changedFields, changedFunctions);
            if (groupFunctionBinding.isChanged(changedFields)) {
                processChangesForPossibleChangedGroups(rows);
            } else {
                processChangesForStableGroups(rows);
            }
            fire();
            cache.updateSelected(rows, changedFields);
        }

        private void processChangesForPossibleChangedGroups(final IntIterable rows) {
            rows.forEach(
                    row -> {
                        final int newGroup = groupFunction.group(row);
                        final int oldGroup = groupMapping.groupOfInboundRow(row);
                        if (newGroup == oldGroup) {
                            processGroupUpdateFromChange(newGroup, row);
                        } else {
                            processRowRemovedFromGroup(oldGroup, row); // must remove first
                            processRowAddedToGroup(newGroup, row);
                        }
                    });
            if (rowsAddedToGroups.isEmpty() && rowsRemovedFromGroups.isEmpty()) {
                updateChangedFunctions();
            } else {
                updateAllFunctions();
            }
        }

        private void processChangesForStableGroups(final IntIterable rows) {
            rows.forEach(
                    row -> {
                        final int oldGroup = groupMapping.groupOfInboundRow(row);
                        processGroupUpdateFromChange(oldGroup, row);
                    });
            updateChangedFunctions();
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
        }

        private void processRowAddedToGroup(final int group, final int row) {
            final int oldCount = groupMapping.groupCount(group);
            groupMapping.mapRowToGroup(row, group);
            if (oldCount == 0) {
                stateChange.addRow(group);
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

        private void updateChangedFunctions() {
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
