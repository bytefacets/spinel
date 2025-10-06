// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.vaadin.data;

import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.printer.RendererRegistry;
import com.bytefacets.spinel.printer.ValueRenderer;
import com.bytefacets.spinel.schema.SchemaField;
import com.vaadin.flow.function.ValueProvider;

/**
 * Convenience class for calling the ValueRenderer for a Field and returning the String
 * representation of the value. The ValueRender is typically derived from the metadata on the
 * SchemaField which contains formatting hints. The ValueRenderer can do things like represent longs
 * and timestamp strings, and handle durations, integer formatting, etc.
 *
 * @see com.bytefacets.spinel.schema.AttributeConstants
 */
public final class FormattedValueProvider implements ValueProvider<TransformRow, String> {
    private static final RendererRegistry DEFAULT_REGISTRY = RendererRegistry.rendererRegistry();
    private final StringBuilder sb = new StringBuilder();
    private final ValueRenderer fieldRenderer;

    public FormattedValueProvider(final SchemaField schemaField) {
        this(DEFAULT_REGISTRY.renderer(schemaField));
    }

    public FormattedValueProvider(final ValueRenderer fieldRenderer) {
        this.fieldRenderer = requireNonNull(fieldRenderer, "fieldRenderer");
    }

    @Override
    public String apply(final TransformRow transformRow) {
        sb.setLength(0);
        fieldRenderer.render(sb, transformRow.getRow());
        final String rendered = sb.toString();
        sb.setLength(0);
        return rendered;
    }
}
