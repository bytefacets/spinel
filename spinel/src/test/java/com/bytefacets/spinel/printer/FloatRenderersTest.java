// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.printer;

import static com.bytefacets.spinel.schema.Metadata.metadata;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.bytefacets.spinel.schema.AttributeConstants;
import com.bytefacets.spinel.schema.FloatField;
import com.bytefacets.spinel.schema.SchemaField;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FloatRenderersTest {
    private final Map<String, Object> attrs = new HashMap<>();
    private final RendererRegistry registry = new RendererRegistry();

    @Nested
    class NaturalRendererTests {
        @BeforeEach
        void setUp() {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Natural);
        }

        @ParameterizedTest
        @CsvSource(
                delimiter = '|',
                value = {"0|5,679", "1|5,678.7", "2|5,678.66", "-1|5,678.658"})
        void shouldRenderValuesWithDisplayPrecision(
                final byte displayPrecision, final String expected) {
            AttributeConstants.setDisplayPrecision(attrs, displayPrecision);
            assertThat(render(5678.6578f), equalTo(expected));
        }

        @ParameterizedTest
        @CsvSource(
                delimiter = '|',
                value = {"####.00000|8.65780"})
        void shouldRenderValuesWithDisplayFormat(final String format, final String expected) {
            AttributeConstants.setDisplayFormat(attrs, format);
            assertThat(render(8.6578f), equalTo(expected));
        }

        // formatting:off
        @ParameterizedTest
        @CsvSource(delimiter = '|', value = {
                "-1|78.6578|79",            "-3|78.6578|79",
                "0|78.6578|79",             "3|78.6578|78.658",
                "0|5678.6578|6k",           "3|5678.6578|5.679k",
                "0|5678541.6578|6M",        "3|5678541.6578|5.679M",
                "0|5678525241.6578|6B",     "3|5678525241.6578|5.679B",
                "0|5678525826241.6578|6T",  "3|5678525826241.6578|5.679T",
                "2|-5678541.6578|-5.68M",
        })
        // formatting:on
        void shouldRenderValuesWithKmbtFormat(
                final byte displayPrecision, final float value, final String expected) {
            AttributeConstants.setDisplayFormat(attrs, AttributeConstants.DisplayFormats.kMBT);
            AttributeConstants.setDisplayPrecision(attrs, displayPrecision);
            assertThat(render(value), equalTo(expected));
        }
    }

    @Nested
    class PercentRendererTests {
        @BeforeEach
        void setUp() {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Percent);
        }

        @ParameterizedTest
        @CsvSource(
                delimiter = '|',
                value = {"0|567,865%", "1|567,865.0%"})
        void shouldRenderValuesWithDisplayPrecision(
                final byte displayPrecision, final String expected) {
            AttributeConstants.setDisplayPrecision(attrs, displayPrecision);
            assertThat(render(5678.65f), equalTo(expected));
        }

        @ParameterizedTest
        @CsvSource(
                delimiter = '|',
                value = {"####.00000%|56.80000%"})
        void shouldRenderValuesWithDisplayFormat(final String format, final String expected) {
            AttributeConstants.setDisplayFormat(attrs, format);
            assertThat(render(0.568f), equalTo(expected));
        }
    }

    private String render(final float value) {
        final ValueRenderer renderer = registry.renderer(schemaField(value));
        final StringBuilder sb = new StringBuilder();
        renderer.render(sb, 0);
        return sb.toString();
    }

    private SchemaField schemaField(final float value) {
        return SchemaField.schemaField(0, "test", field(value), metadata(attrs));
    }

    private static FloatField field(final float value) {
        return row -> value;
    }
}
