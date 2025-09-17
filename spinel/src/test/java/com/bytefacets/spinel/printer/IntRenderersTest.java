// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.printer;

import static com.bytefacets.spinel.schema.Metadata.metadata;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.bytefacets.collections.types.IntType;
import com.bytefacets.spinel.schema.AttributeConstants;
import com.bytefacets.spinel.schema.IntField;
import com.bytefacets.spinel.schema.SchemaField;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class IntRenderersTest {
    private final Map<String, Object> attrs = new HashMap<>();
    private final RendererRegistry registry = new RendererRegistry();

    @Nested
    class NaturalTests {
        @ParameterizedTest
        @CsvSource({"-1,-1", "56,56"})
        void shouldRenderNatural(final int value, final String expected) {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Natural);
            assertThat(render(value), equalTo(expected));
        }
    }

    @Nested
    class IdTests {
        @ParameterizedTest
        @CsvSource({"-1,-1", "56,56"})
        void shouldRenderId(final int value, final String expected) {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Id);
            assertThat(render(value), equalTo(expected));
        }
    }

    @Nested
    class QuantityTests {
        @ParameterizedTest
        @CsvSource({"12045,12k", "56,56"})
        void shouldRenderQuantityWithKmbt(final int value, final String expected) {
            AttributeConstants.setDisplayFormat(attrs, AttributeConstants.DisplayFormats.kMBT);
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Quantity);
            assertThat(render(value), equalTo(expected));
        }

        @Test
        void shouldRenderQuantityWithFormat() {
            AttributeConstants.setDisplayFormat(attrs, "#,### hi");
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Quantity);
            assertThat(render(12045), equalTo("12,045 hi"));
        }

        @Test
        void shouldRenderQuantityWithDefault() {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Quantity);
            assertThat(render(12045), equalTo("12,045"));
            assertThat(render(469), equalTo("469"));
        }
    }

    @Nested
    class TextTests {
        @ParameterizedTest
        @CsvSource({"HIGH", "LOW!"})
        void shouldRenderTextBE(final String text) {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Text);
            AttributeConstants.setDisplayFormat(attrs, AttributeConstants.DisplayFormats.BigEndian);
            final int value = IntType.readBE(text.getBytes(StandardCharsets.UTF_8), 0);
            assertThat(render(value), equalTo(text));
        }

        @ParameterizedTest
        @CsvSource({"HIGH", "LOW!"})
        void shouldRenderTextLE(final String text) {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Text);
            AttributeConstants.setDisplayFormat(
                    attrs, AttributeConstants.DisplayFormats.LittleEndian);
            final int value = IntType.readLE(text.getBytes(StandardCharsets.UTF_8), 0);
            assertThat(render(value), equalTo(text));
        }
    }

    @Nested
    class FlagTests {
        @ParameterizedTest
        @ValueSource(ints = {15, 6, 0, -32768, 32767, Integer.MAX_VALUE, Integer.MIN_VALUE})
        void shouldRenderText(final int value) {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Flag);
            assertThat(render(value), equalTo(expectedBits(value)));
        }

        private static String expectedBits(final int value) {
            final String bin =
                    String.format("%32s", Integer.toBinaryString(value)).replace(' ', '0');
            final List<Integer> setBits = new ArrayList<>();
            for (int i = 0; i < bin.length(); i++) {
                if (bin.charAt(bin.length() - 1 - i) == '1') {
                    setBits.add(i);
                }
            }
            return setBits.toString().replace('[', '{').replace(']', '}').replace(" ", "");
        }
    }

    private String render(final int value) {
        final ValueRenderer renderer = registry.renderer(schemaField(value));
        final StringBuilder sb = new StringBuilder();
        renderer.render(sb, 0);
        return sb.toString();
    }

    private SchemaField schemaField(final int value) {
        return SchemaField.schemaField(0, "test", field(value), metadata(attrs));
    }

    private static IntField field(final int value) {
        return row -> value;
    }
}
