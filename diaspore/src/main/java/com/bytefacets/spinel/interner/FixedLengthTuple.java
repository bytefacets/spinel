// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.interner;

import java.util.Arrays;

@SuppressWarnings("NeedBraces")
final class FixedLengthTuple implements OpaqueTuple {
    private final byte[] value;

    FixedLengthTuple(final byte[] value) {
        this.value = value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final FixedLengthTuple that = (FixedLengthTuple) o;
        return Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }
}
