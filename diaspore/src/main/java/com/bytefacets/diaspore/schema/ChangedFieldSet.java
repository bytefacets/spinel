// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.schema;

import com.bytefacets.collections.functional.IntConsumer;
import java.util.BitSet;

public interface ChangedFieldSet {
    boolean isChanged(int fieldId);

    int size();

    void forEach(IntConsumer consumer);

    boolean intersects(BitSet dependencies);
}
