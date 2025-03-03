// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.transform;

public interface TransformNode<T> {
    String name();

    T operator();
}
