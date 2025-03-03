// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.diaspore.schema.ChangedFieldSet;
import com.bytefacets.diaspore.schema.Schema;
import javax.annotation.Nullable;

public interface TransformInput {
    default void setSource(@Nullable TransformOutput output) {}

    void schemaUpdated(@Nullable Schema schema);

    void rowsAdded(IntIterable rows);

    void rowsChanged(IntIterable rows, ChangedFieldSet changedFields);

    void rowsRemoved(IntIterable rows);
}
