// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.printer;

import static com.bytefacets.spinel.schema.Metadata.metadata;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.bytefacets.spinel.schema.AttributeConstants;
import com.bytefacets.spinel.schema.CharField;
import com.bytefacets.spinel.schema.SchemaField;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class CharRenderersTest {
    private final Map<String, Object> attrs = new HashMap<>();
    private final RendererRegistry registry = new RendererRegistry();

    @Nested
    class NaturalTests {
        @Test
        void shouldRenderNatural() {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Natural);
            assertThat(render('A'), equalTo("A"));
        }
    }

    @Nested
    class IdTests {
        @Test
        void shouldRenderId() {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Id);
            assertThat(render('A'), equalTo("A"));
        }
    }

    @Nested
    class QuantityTests {
        @ParameterizedTest
        @CsvSource({"12045,12k", "56,56"})
        void shouldRenderQuantityWithKmbt(final int value, final String expected) {
            AttributeConstants.setDisplayFormat(attrs, AttributeConstants.DisplayFormats.kMBT);
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Quantity);
            assertThat(render((char) value), equalTo(expected));
        }

        @Test
        void shouldRenderQuantityWithFormat() {
            AttributeConstants.setDisplayFormat(attrs, "#,### hi");
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Quantity);
            assertThat(render((char) 12045), equalTo("12,045 hi"));
            assertThat(render((char) 48613), equalTo("48,613 hi"));
        }

        @Test
        void shouldRenderQuantityWithDefault() {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Quantity);
            assertThat(render((char) 12045), equalTo("12,045"));
            assertThat(render((char) 48613), equalTo("48,613"));
        }
    }

    @Nested
    class TextTests {
        @Test
        void shouldRenderText() {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Text);
            assertThat(render('A'), equalTo("A"));
        }
    }

    private String render(final char value) {
        final ValueRenderer renderer = registry.renderer(schemaField(value));
        final StringBuilder sb = new StringBuilder();
        renderer.render(sb, 0);
        return sb.toString();
    }

    private SchemaField schemaField(final char value) {
        return SchemaField.schemaField(0, "test", field(value), metadata(attrs));
    }

    private static CharField field(final char value) {
        return row -> value;
    }
}
