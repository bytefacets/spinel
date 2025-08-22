// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.comms.send;

import com.bytefacets.spinel.TransformOutput;
import jakarta.annotation.Nullable;

public interface OutputRegistry {
    @Nullable
    TransformOutput lookup(String name);
}
