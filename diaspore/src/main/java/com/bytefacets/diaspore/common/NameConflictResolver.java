// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.common;

import java.util.function.Predicate;

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
