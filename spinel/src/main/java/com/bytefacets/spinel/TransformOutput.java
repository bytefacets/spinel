// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel;

import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.transform.InputProvider;
import jakarta.annotation.Nullable;

public interface TransformOutput {
    void attachInput(TransformInput input);

    default void attachInput(InputProvider inputProvider) {
        attachInput(inputProvider.input());
    }

    @Nullable
    Schema schema();

    RowProvider rowProvider();

    void detachInput(TransformInput input);

    default void detachInput(InputProvider inputProvider) {
        detachInput(inputProvider.input());
    }
}
