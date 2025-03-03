// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.comms.send;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.diaspore.schema.ChangedFieldSet;
import com.bytefacets.diaspore.schema.Schema;

public interface ChangeEncoder<T> {
    T encodeSchema(Schema schema);

    T encodeAdd(IntIterable rows);

    T encodeChange(IntIterable rows, ChangedFieldSet fieldSet);

    T encodeRemove(IntIterable rows);
}
