// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.interner;

import java.util.Arrays;
import java.util.Objects;

@SuppressWarnings("NeedBraces")
final class MixedTuple implements OpaqueTuple {
    private final byte[] value;
    private final Object[] objects;

    MixedTuple(final byte[] value, final Object[] objects) {
        this.value = value;
        this.objects = objects;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final MixedTuple that = (MixedTuple) o;
        return Arrays.equals(value, that.value) && Arrays.equals(objects, that.objects);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(value), Arrays.hashCode(objects));
    }
}
