// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel;

import com.bytefacets.spinel.schema.Schema;
import jakarta.annotation.Nullable;

public interface TransformOutput {
    void attachInput(TransformInput input);

    @Nullable
    Schema schema();

    RowProvider rowProvider();

    void detachInput(TransformInput input);
}
