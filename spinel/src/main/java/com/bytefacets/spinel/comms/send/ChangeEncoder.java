// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.comms.send;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.spinel.schema.ChangedFieldSet;
import com.bytefacets.spinel.schema.Schema;

public interface ChangeEncoder<T> {
    T encodeSchema(Schema schema);

    T encodeAdd(IntIterable rows);

    T encodeChange(IntIterable rows, ChangedFieldSet fieldSet);

    T encodeRemove(IntIterable rows);
}
