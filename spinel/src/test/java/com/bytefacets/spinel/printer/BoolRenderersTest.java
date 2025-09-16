// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.printer;

import static com.bytefacets.spinel.schema.Metadata.metadata;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.bytefacets.spinel.schema.BoolField;
import com.bytefacets.spinel.schema.SchemaField;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BoolRenderersTest {
    private final Map<String, Object> attrs = new HashMap<>();
    private final RendererRegistry registry = new RendererRegistry();

    @Test
    void shouldRenderNatural() {
        assertThat(render(true), equalTo("true"));
        assertThat(render(false), equalTo("false"));
    }

    private String render(final boolean value) {
        final ValueRenderer renderer = registry.renderer(schemaField(value));
        final StringBuilder sb = new StringBuilder();
        renderer.render(sb, 0);
        return sb.toString();
    }

    private SchemaField schemaField(final boolean value) {
        return SchemaField.schemaField(0, "test", field(value), metadata(attrs));
    }

    private static BoolField field(final boolean value) {
        return row -> value;
    }
}
