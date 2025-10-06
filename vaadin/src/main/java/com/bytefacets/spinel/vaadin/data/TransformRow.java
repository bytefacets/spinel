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
}
