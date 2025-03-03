// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore;

import com.bytefacets.diaspore.schema.Schema;
import javax.annotation.Nullable;

public interface TransformOutput {
    void attachInput(TransformInput input);

    @Nullable
    Schema schema();

    RowProvider rowProvider();

    void detachInput(TransformInput input);
}
