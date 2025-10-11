// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.table;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import com.bytefacets.spinel.facade.FieldNamingStrategy;
import com.bytefacets.spinel.schema.AttributeConstants;
import com.bytefacets.spinel.schema.Metadata;
import com.bytefacets.spinel.schema.ValueMetadata;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StructTableBuilderTest {
    private final Map<String, Object> attrs = new HashMap<>(4);

    @Test
    void shouldReplaceMetadata() {
        attrs.put(AttributeConstants.TimeZone, "UTC");
        final var table =
                StructTableBuilder.table(MyType.class)
                        .replaceMetadata("FieldExample1", Metadata.metadata(attrs))
                        .build();
        // only TimeZone survives
        assertThat(
                table.schema().field("FieldExample1").metadata(),
                equalTo(Metadata.metadata(attrs)));
    }

    @Test
    void shouldUpdateMetadata() {
        attrs.put(AttributeConstants.TimeZone, "UTC");
        final var table =
                StructTableBuilder.table(MyType.class)
                        .updateMetadata("FieldExample1", Metadata.metadata(attrs))
                        .build();
        final Map<String, Object> expectedAttrs = new HashMap<>(4);
        expectedAttrs.put(AttributeConstants.ContentType, AttributeConstants.ContentTypes.Id);
        expectedAttrs.put(AttributeConstants.TimeZone, "UTC");
        assertThat(
                table.schema().field("FieldExample1").metadata(),
                equalTo(Metadata.metadata(expectedAttrs)));
    }

    @Test
    void shouldApplyNamingStrategy() {
        final var table =
                StructTableBuilder.table(MyType.class)
                        .fieldNamingStrategy(FieldNamingStrategy.SnakeCase)
                        .build();
        final List<String> fieldNames = new ArrayList<>(3);
        table.schema().forEachField(f -> fieldNames.add(f.name()));
        assertThat(fieldNames, containsInAnyOrder("field_example_1", "field_example_2"));
    }

    // formatting:off
    interface MyType {
        @ValueMetadata(contentType = AttributeConstants.ContentTypes.Id)
        MyType setFieldExample1(int value); int getFieldExample1();
        MyType setFieldExample2(int value); int getFieldExample2();
    }
    // formatting:on
}
