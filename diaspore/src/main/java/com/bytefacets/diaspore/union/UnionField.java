// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.union;

import com.bytefacets.diaspore.schema.Field;
import javax.annotation.Nullable;

public interface UnionField extends Field {
    void setField(int index, @Nullable Field field);
}
