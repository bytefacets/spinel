// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.testing;

import com.bytefacets.diaspore.table.TableRow;

interface ObjectFieldAdapter {
    void apply(TableRow row, Object value);
}
