// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.comms.send;

import com.bytefacets.diaspore.TransformOutput;
import javax.annotation.Nullable;

public interface OutputRegistry {
    @Nullable
    TransformOutput lookup(String name);
}
