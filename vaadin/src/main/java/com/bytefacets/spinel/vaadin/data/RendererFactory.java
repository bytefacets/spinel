// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.vaadin.data;

import static com.bytefacets.spinel.vaadin.data.LastValueSupportFactory.lastValueSupport;
import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.printer.RendererRegistry;
import com.bytefacets.spinel.printer.ValueRenderer;
import com.bytefacets.spinel.schema.SchemaField;
import com.vaadin.flow.data.renderer.LitRenderer;
import com.vaadin.flow.data.renderer.Renderer;
import com.vaadin.flow.data.renderer.TextRenderer;

/** Helper methods for creating Vaadin renderers that reach into a schema to get the values. */
public final class RendererFactory {
    private RendererFactory() {}

    /**
     * Creates a TextRenderer based on SchemaField. The SchemaField metadata is used to create a
     * ValueRenderer.
     *
     * @see RendererRegistry#renderer(SchemaField)
     */
    public static Renderer<TransformRow> textRenderer(final SchemaField field) {
        return new SchemaFieldRenderer(new FormattedValueProvider(field));
    }

    public static Renderer<TransformRow> textRenderer(final ValueRenderer valueRenderer) {
        return new SchemaFieldRenderer(new FormattedValueProvider(valueRenderer));
    }

    /**
     * A Renderer which applies the given changedClassName in the css when the new value is
     * different from the old value.
     *
     * @param field the schema field to operate over
     * @param changedClassName the css class name in your application which indicates a changed
     *     value
     * @return the renderer which you would set on the Grid.Column in something like
     *     GridAdapter.ColumnSetup
     */
    public static LitRenderer<TransformRow> changedRenderer(
            final SchemaField field, final String changedClassName) {
        final LastValueSupport lastValueSupport =
                lastValueSupport(
                        field, (newValue, oldValue, cmp) -> cmp != 0 ? changedClassName : "");
        return LitRenderer.<TransformRow>of("<div class='${item.changeClass}'>${item.text}</div>")
                .withProperty("text", new FormattedValueProvider(field))
                .withProperty("changeClass", lastValueSupport::evaluate);
    }

    /** Adapts the Formatter to a Vaadin TextRenderer */
    private static final class SchemaFieldRenderer extends TextRenderer<TransformRow> {
        private final FormattedValueProvider formatter;

        private SchemaFieldRenderer(final FormattedValueProvider formatter) {
            this.formatter = requireNonNull(formatter, "formatter");
        }

        @Override
        protected String getFormattedValue(final TransformRow o) {
            return formatter.apply(o);
        }
    }
}
