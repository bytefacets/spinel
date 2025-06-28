// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.join;

import com.bytefacets.diaspore.RowProvider;
import com.bytefacets.diaspore.schema.RowMapper;

interface JoinMapper {
    int NULL_ROW = Integer.MAX_VALUE;

    RowProvider rowProvider();

    JoinInterner interner();

    RowMapper leftMapper();

    RowMapper rightMapper();

    void leftRowAdd(int leftRow);

    void rightRowAdd(int rightRow);

    void leftRowChange(int leftRow, boolean reEvalKey);

    void rightRowChange(int rightRow, boolean reEvalKey);

    void leftRowRemove(int leftRow);

    void rightRowRemove(int rightRow);

    void cleanUpRemovedRow(int outRow);
}
