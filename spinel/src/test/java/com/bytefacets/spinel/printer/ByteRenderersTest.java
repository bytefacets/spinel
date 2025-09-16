// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.printer;

import static com.bytefacets.spinel.schema.Metadata.metadata;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.bytefacets.spinel.schema.AttributeConstants;
import com.bytefacets.spinel.schema.ByteField;
import com.bytefacets.spinel.schema.SchemaField;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ByteRenderersTest {
    private final Map<String, Object> attrs = new HashMap<>();
    private final RendererRegistry registry = new RendererRegistry();

    @Nested
    class NaturalTests {
        @ParameterizedTest
        @CsvSource({"-1,-1", "56,56"})
        void shouldRenderNatural(final byte value, final String expected) {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Natural);
            assertThat(render(value), equalTo(expected));
        }
    }

    @Nested
    class IdTests {
        @ParameterizedTest
        @CsvSource({"-1,-1", "56,56"})
        void shouldRenderId(final byte value, final String expected) {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Id);
            assertThat(render(value), equalTo(expected));
        }
    }

    @Nested
    class QuantityTests {
        @ParameterizedTest
        @CsvSource({"-1,-1", "56,56"})
        void shouldRenderQuantity(final byte value, final String expected) {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Quantity);
            assertThat(render(value), equalTo(expected));
        }
    }

    @Nested
    class TextTests {
        @ParameterizedTest
        @CsvSource({"65,A", "80,P"})
        void shouldRenderText(final byte value, final String expected) {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Text);
            assertThat(render(value), equalTo(expected));
        }
    }

    @Nested
    class FlagTests {
        @ParameterizedTest
        @CsvSource(
                delimiter = '|',
                value = {
                    "15|{0,1,2,3}",
                    "6|{1,2}",
                    "0|{}",
                    "-121|{0,1,2,7}",
                    "-128|{7}",
                    "127|{0,1,2,3,4,5,6}"
                })
        void shouldRenderText(final byte value, final String expected) {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Flag);
            assertThat(render(value), equalTo(expected));
        }
    }

    private String render(final byte value) {
        final ValueRenderer renderer = registry.renderer(schemaField(value));
        final StringBuilder sb = new StringBuilder();
        renderer.render(sb, 0);
        return sb.toString();
    }

    private SchemaField schemaField(final byte value) {
        return SchemaField.schemaField(0, "test", field(value), metadata(attrs));
    }

    private static ByteField field(final byte value) {
        return row -> value;
    }
}
