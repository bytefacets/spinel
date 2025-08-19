// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.filter;

import com.bytefacets.spinel.schema.SchemaBindable;

public interface RowPredicate extends SchemaBindable {

    boolean testRow(int row);
}
