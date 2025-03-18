// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.common;

/**
 * A interface used by components when binding to a schema to indicate when it depends on a field.
 */
public interface FieldDependencySet {
    void dependsOnFieldId(int fieldId);
}
