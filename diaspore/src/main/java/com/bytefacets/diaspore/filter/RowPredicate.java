// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.filter;

import com.bytefacets.diaspore.schema.SchemaBindable;

public interface RowPredicate extends SchemaBindable {

    boolean testRow(int row);
}
