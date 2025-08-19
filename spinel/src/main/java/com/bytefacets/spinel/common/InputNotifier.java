// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.common;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.spinel.schema.ChangedFieldSet;

/** Used to abstract an operator's output from the connected inputs. */
public interface InputNotifier {
    void notifyAdds(IntIterable rows);

    void notifyChanges(IntIterable rows, ChangedFieldSet changedFields);

    void notifyRemoves(IntIterable rows);
}
