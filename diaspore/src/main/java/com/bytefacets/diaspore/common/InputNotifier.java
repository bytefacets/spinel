// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.common;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.diaspore.schema.ChangedFieldSet;

public interface InputNotifier {
    void notifyAdds(IntIterable rows);

    void notifyChanges(IntIterable rows, ChangedFieldSet changedFields);

    void notifyRemoves(IntIterable rows);
}
