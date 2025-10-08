// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.vaadin.grid;

import com.bytefacets.spinel.schema.SchemaField;
import com.bytefacets.spinel.vaadin.data.TransformRow;
import com.vaadin.flow.component.grid.Grid;
import com.vaadin.flow.function.ValueProvider;

/**
 * Callback that allows additional customization of Grid columns. By default, the GridAdapter will
 * set the heading, the text alignment, and create a TextRenderer specific to the SchemaField's
 * metadata. But if you want more customization like LitRenderer, or anything else, you can register
 * an implementation of this interface to customize the column further.
 */
public interface ColumnSetup {
    void setUp(
            Grid.Column<TransformRow> column,
            SchemaField schemaField,
            ValueProvider<TransformRow, Object> valueProvider);
}
