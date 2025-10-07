// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.vaadin.data;

/** Wraps a row reference for use in DataProviders */
public final class TransformRow {
    private int row = -1;

    public TransformRow() {}

    public TransformRow(final int row) {
        this.row = row;
    }

    public int getRow() {
        return row;
    }

    public void setRow(final int row) {
        this.row = row;
    }

    @SuppressWarnings("NeedBraces")
    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (!(o instanceof final TransformRow that)) return false;
        return row == that.row;
    }

    @Override
    public int hashCode() {
        return row;
    }
}
