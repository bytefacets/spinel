// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.validation;

import java.util.List;
import java.util.stream.Stream;

public record Key(List<Object> values) {
    public static Key key(Object... values) {
        return new Key(Stream.of(values).toList());
    }
}
