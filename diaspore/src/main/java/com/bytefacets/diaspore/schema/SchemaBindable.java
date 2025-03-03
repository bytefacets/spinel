// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.schema;

public interface SchemaBindable {
    void bindToSchema(FieldResolver fieldResolver);

    void unbindSchema();
}
