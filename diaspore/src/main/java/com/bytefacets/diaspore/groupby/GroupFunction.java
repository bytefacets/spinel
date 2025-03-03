// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.groupby;

import com.bytefacets.diaspore.schema.FieldResolver;

public interface GroupFunction {
    void bindToSchema(FieldResolver fieldResolver);

    void unbindSchema();

    int group(int row);

    void onEmptyGroup(int group);
}
