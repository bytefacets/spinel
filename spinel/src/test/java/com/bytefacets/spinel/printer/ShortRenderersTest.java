// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.printer;

import static com.bytefacets.spinel.schema.Metadata.metadata;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.bytefacets.collections.types.Pack;
import com.bytefacets.collections.types.ShortType;
import com.bytefacets.spinel.schema.AttributeConstants;
import com.bytefacets.spinel.schema.SchemaField;
import com.bytefacets.spinel.schema.ShortField;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ShortRenderersTest {
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
        @CsvSource({"12045,12k", "56,56"})
        void shouldRenderQuantityWithKmbt(final short value, final String expected) {
            AttributeConstants.setDisplayFormat(attrs, AttributeConstants.DisplayFormats.kMBT);
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Quantity);
            assertThat(render(value), equalTo(expected));
        }

        @Test
        void shouldRenderQuantityWithFormat() {
            AttributeConstants.setDisplayFormat(attrs, "#,### hi");
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Quantity);
            assertThat(render((short) 12045), equalTo("12,045 hi"));
        }

        @Test
        void shouldRenderQuantityWithDefault() {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Quantity);
            assertThat(render((short) 12045), equalTo("12,045"));
            assertThat(render((short) 469), equalTo("469"));
        }
    }

    @Nested
    class TextTests {
        @ParameterizedTest
        @CsvSource({"HI", "LO"})
        void shouldRenderTextBE(final String text) {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Text);
            AttributeConstants.setDisplayFormat(attrs, AttributeConstants.DisplayFormats.BigEndian);
            final short value = ShortType.readBE(text.getBytes(StandardCharsets.UTF_8), 0);
            assertThat(render(value), equalTo(text));
        }

        @ParameterizedTest
        @CsvSource({"HI", "LO"})
        void shouldRenderTextLE(final String text) {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Text);
            AttributeConstants.setDisplayFormat(
                    attrs, AttributeConstants.DisplayFormats.LittleEndian);
            final short value = ShortType.readLE(text.getBytes(StandardCharsets.UTF_8), 0);
            assertThat(render(value), equalTo(text));
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
                    "-32768|{15}",
                    "32767|{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14}"
                })
        void shouldRenderText(final short value, final String expected) {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Flag);
            assertThat(render(value), equalTo(expected));
        }
    }

    @Nested
    class PackTests {
        @Test
        void shouldRenderPack2() {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Packed2);
            assertThat(render(Pack.packToShort((byte) 64, (byte) -76)), equalTo("(64,-76)"));
            assertThat(
                    render(Pack.packToShort(Byte.MIN_VALUE, Byte.MAX_VALUE)),
                    equalTo("(-128,127)"));
            assertThat(
                    render(Pack.packToShort(Byte.MAX_VALUE, Byte.MIN_VALUE)),
                    equalTo("(127,-128)"));
        }
    }

    private String render(final short value) {
        final ValueRenderer renderer = registry.renderer(schemaField(value));
        final StringBuilder sb = new StringBuilder();
        renderer.render(sb, 0);
        return sb.toString();
    }

    private SchemaField schemaField(final short value) {
        return SchemaField.schemaField(0, "test", field(value), metadata(attrs));
    }

    private static ShortField field(final short value) {
        return row -> value;
    }
}
