// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.common;

import java.util.function.Predicate;

/** When combining schemas, such as in a Join, this interface is used to resolve name conflicts. */
public interface NameConflictResolver {
    default String resolveNameConflict(
            final String attemptedName, final Predicate<String> usableNameTest) {
        int count = 1;
        String newName = attemptedName + "_" + (count++);
        while (!usableNameTest.test(newName)) {
            newName = attemptedName + "_" + (count++);
        }
        return newName;
    }
}
