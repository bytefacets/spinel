// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.groupby;

import com.bytefacets.collections.arrays.IntArray;
import com.bytefacets.collections.bi.CompactOneToMany;
import com.bytefacets.diaspore.schema.IntField;
import com.bytefacets.diaspore.schema.RowIdentityField;
import com.bytefacets.diaspore.schema.RowMapper;

final class GroupMapping {
    private static final int NO_GROUP = -1;
    private int[] rowToGroupId;
    private final CompactOneToMany groupToChildRowMapping;
    private final RowMapper passThruRowMapper = new FirstGroupRowMapper();

    GroupMapping(final int initialGroupSize, final int initialInRowSize) {
        this.rowToGroupId = IntArray.create(initialInRowSize, NO_GROUP);
        this.groupToChildRowMapping =
                new CompactOneToMany(initialGroupSize, initialInRowSize, true);
    }

    void reset() {
        IntArray.fill(rowToGroupId, NO_GROUP);
    }

    void mapRowToGroup(final int row, final int group) {
        rowToGroupId = IntArray.ensureEntry(rowToGroupId, row);
        rowToGroupId[row] = group;
        groupToChildRowMapping.put(group, row);
    }

    int groupOfInboundRow(final int row) {
        return row < rowToGroupId.length ? rowToGroupId[row] : NO_GROUP;
    }

    int groupCount(final int group) {
        return groupToChildRowMapping.withLeft(group).count();
    }

    void unmapRow(final int row) {
        final int oldGroup = rowToGroupId[row];
        groupToChildRowMapping.remove(oldGroup, row);
        rowToGroupId[row] = NO_GROUP;
    }

    IntField parentGroupIdField() {
        return RowIdentityField.rowIdentityField();
    }

    IntField countField() {
        return row -> groupToChildRowMapping.withLeft(row).count();
    }

    IntField childGroupIdField() {
        return this::groupOfInboundRow;
    }

    RowMapper passThroughFieldMapper() {
        return passThruRowMapper;
    }

    private class FirstGroupRowMapper implements RowMapper {
        @Override
        public int sourceRowOf(final int group) {
            // iterator is re-used
            final var it = groupToChildRowMapping.withLeft(group).valueIterator();
            return it.hasNext() ? it.next() : -1;
        }
    }
}
