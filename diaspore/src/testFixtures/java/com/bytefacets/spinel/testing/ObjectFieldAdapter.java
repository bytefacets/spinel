// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.testing;

import com.bytefacets.spinel.table.TableRow;

interface ObjectFieldAdapter {
    void apply(TableRow row, Object value);
}
