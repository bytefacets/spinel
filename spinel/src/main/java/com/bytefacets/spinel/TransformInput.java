// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.spinel.schema.ChangedFieldSet;
import com.bytefacets.spinel.schema.Schema;
import jakarta.annotation.Nullable;

public interface TransformInput {
    default void setSource(@Nullable TransformOutput output) {}

    void schemaUpdated(@Nullable Schema schema);

    void rowsAdded(IntIterable rows);

    void rowsChanged(IntIterable rows, ChangedFieldSet changedFields);

    void rowsRemoved(IntIterable rows);
}
