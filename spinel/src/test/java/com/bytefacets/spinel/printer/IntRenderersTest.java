// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.printer;

import static com.bytefacets.spinel.schema.Metadata.metadata;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.bytefacets.collections.types.IntType;
import com.bytefacets.collections.types.Pack;
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

    @Nested
    class PackTests {
        @Test
        void shouldRenderPack2() {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Packed2);
            assertThat(
                    render(Pack.packToInt((short) 3653, (short) -8782)), equalTo("(3653,-8782)"));
            assertThat(
                    render(Pack.packToInt(Short.MAX_VALUE, Short.MIN_VALUE)),
                    equalTo("(32767,-32768)"));
            assertThat(
                    render(Pack.packToInt(Short.MIN_VALUE, Short.MAX_VALUE)),
                    equalTo("(-32768,32767)"));
        }

        @Test
        void shouldRenderPack4() {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Packed4);
            assertThat(render(pack4(1, 10, 32, -67)), equalTo("(1,10,32,-67)"));
            assertThat(
                    render(pack4(Byte.MIN_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MIN_VALUE)),
                    equalTo("(-128,127,127,-128)"));
        }

        private int pack4(final int a, final int b, final int c, final int d) {
            return Pack.packToInt((byte) a, (byte) b, (byte) c, (byte) d);
        }
    }

    @Nested
    class DateTests {
        @Test
        void shouldRenderDate() {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Date);
            assertThat(render(20250917), equalTo("2025-09-17"));
            assertThat(render(20250905), equalTo("2025-09-05"));
            assertThat(render(20251105), equalTo("2025-11-05"));
            // make no claims about validations
            assertThat(render(7894567), equalTo("789-45-67"));
            assertThat(render(Integer.MAX_VALUE), equalTo("214748-36-47"));
            // make no claims about negative numbers
        }
    }

    @Nested
    class DurationTests {
        @ParameterizedTest
        @CsvSource({
            "1,1,9123456,2534:17:36",
            "1,1,86465,24:01:05",
            "1,2,86465,24:01",
            "2,1,1478,24:38:00",
            "2,2,1478,24:38",
            "-3,1,9123456,02:32:03",
            "-3,1,-9123456,-02:32:03"
        })
        void shouldRenderDate(
                final byte valuePrecision,
                final byte displayPrecision,
                final int value,
                final String expected) {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Duration);
            AttributeConstants.setDisplayPrecision(attrs, displayPrecision);
            AttributeConstants.setValuePrecision(attrs, valuePrecision);
            assertThat(render(value), equalTo(expected));
        }

        @Test
        void shouldNotAppendAnythingWhenAtMinOrMax() {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Duration);
            assertThat(render(Integer.MAX_VALUE), equalTo(""));
            assertThat(render(Integer.MIN_VALUE), equalTo(""));
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
