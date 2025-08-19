// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.groupby;

import com.bytefacets.spinel.schema.FieldResolver;

public interface GroupFunction {
    void bindToSchema(FieldResolver fieldResolver);

    void unbindSchema();

    int group(int row);

    void onEmptyGroup(int group);
}
