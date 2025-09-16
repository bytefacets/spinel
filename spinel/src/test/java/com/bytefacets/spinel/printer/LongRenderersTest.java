// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.printer;

import static com.bytefacets.spinel.schema.Metadata.metadata;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.bytefacets.collections.types.LongType;
import com.bytefacets.spinel.schema.AttributeConstants;
import com.bytefacets.spinel.schema.LongField;
import com.bytefacets.spinel.schema.SchemaField;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class LongRenderersTest {
    private final Map<String, Object> attrs = new HashMap<>();
    private final RendererRegistry registry = new RendererRegistry();
    private final byte[] data = new byte[8];

    @Nested
    class NaturalTests {
        @ParameterizedTest
        @CsvSource({"88276827682245452,88276827682245452", "-287687682,-287687682"})
        void shouldRenderLongAsText(final long value, final String expected) {
            assertThat(render(value), equalTo(expected));
        }

        @Test
        void shouldRenderWithDisplayFormat() {
            AttributeConstants.setDisplayFormat(attrs, "#,###");
            assertThat(render(88276827682245452L), equalTo("88,276,827,682,245,452"));
        }
    }

    @Nested
    class IdTests {
        @ParameterizedTest
        @CsvSource({"88276827682245452,88276827682245452", "-287687682,-287687682"})
        void shouldRenderIdWithoutFormatting(final long value, final String expected) {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Id);
            assertThat(render(value), equalTo(expected));
        }
    }

    @Nested
    class LittleEndianTextTests {
        @BeforeEach
        void setUp() {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Text);
        }

        @ParameterizedTest
        @ValueSource(strings = {"SOMELONG", "ThisFun!"})
        public void shouldRenderLongAsText(final String expected) {
            AttributeConstants.setDisplayFormat(
                    attrs, AttributeConstants.DisplayFormats.LittleEndian);
            System.arraycopy(expected.getBytes(StandardCharsets.UTF_8), 0, data, 0, 8);
            assertThat(render(LongType.readLE(data, 0)), equalTo(expected));
        }
    }

    @Nested
    class BigEndianTextTests {
        @BeforeEach
        void setUp() {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Text);
        }

        @ParameterizedTest
        @ValueSource(strings = {"SOMELONG", "ThisFun!"})
        public void shouldRenderLongAsText(final String expected) {
            AttributeConstants.setDisplayFormat(attrs, AttributeConstants.DisplayFormats.BigEndian);
            System.arraycopy(expected.getBytes(StandardCharsets.UTF_8), 0, data, 0, 8);
            assertThat(render(LongType.readBE(data, 0)), equalTo(expected));
        }
    }

    private String render(final long value) {
        final ValueRenderer renderer = registry.renderer(schemaField(value));
        final StringBuilder sb = new StringBuilder();
        renderer.render(sb, 0);
        return sb.toString();
    }

    private SchemaField schemaField(final long value) {
        return SchemaField.schemaField(0, "test", field(value), metadata(attrs));
    }

    private static LongField field(final long value) {
        return row -> value;
    }
}
