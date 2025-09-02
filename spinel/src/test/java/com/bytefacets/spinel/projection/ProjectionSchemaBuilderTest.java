// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.projection;

import static com.bytefacets.spinel.schema.FieldList.fieldList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.bytefacets.spinel.exception.OperatorSetupException;
import com.bytefacets.spinel.projection.lib.IntFieldCalculation;
import com.bytefacets.spinel.schema.Field;
import com.bytefacets.spinel.schema.IntField;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.TypeId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ProjectionSchemaBuilderTest {
    private final ProjectionBuilder builder = ProjectionBuilder.projection();
    private final ProjectionDependencyMap dependencyMap = new ProjectionDependencyMap();
    private final Field inAbc = field();
    private final Field inDef = field();

    @Nested
    class SelectionTests {
        @Test
        void shouldHandleIncludedAndOmitted() {
            final var schemaBuilder = builder.include("def").omit("abc").schemaBuilder();
            final var outSchema =
                    schemaBuilder.buildOutboundSchema(
                            schema(Map.of("abc", inAbc, "def", inDef)), dependencyMap);
            assertThat(outSchema.field("def").field(), Matchers.sameInstance(inDef));
            assertThat(outSchema.size(), equalTo(1));
        }

        @Test
        void shouldThrowWhenIncludedFieldNotFound() {
            final var schemaBuilder = builder.include("foo").schemaBuilder();
            assertThrows(
                    OperatorSetupException.class,
                    () ->
                            schemaBuilder.buildOutboundSchema(
                                    schema(Map.of("abc", inAbc, "def", inDef)), dependencyMap));
        }

        @Test
        void shouldIncludeWhenNoSelection() {
            final var schemaBuilder = builder.schemaBuilder();
            final var outSchema =
                    schemaBuilder.buildOutboundSchema(
                            schema(Map.of("abc", inAbc, "def", inDef)), dependencyMap);
            assertThat(outSchema.field("abc").field(), Matchers.sameInstance(inAbc));
            assertThat(outSchema.field("def").field(), Matchers.sameInstance(inDef));
            assertThat(outSchema.size(), equalTo(2));
        }

        @Test
        void shouldIncludeWhenNoOmissions() {
            final var schemaBuilder = builder.include("def").schemaBuilder();
            final var outSchema =
                    schemaBuilder.buildOutboundSchema(
                            schema(Map.of("abc", inAbc, "def", inDef)), dependencyMap);
            assertThat(outSchema.field("def").field(), Matchers.sameInstance(inDef));
            assertThat(outSchema.size(), equalTo(1));
        }

        @Test
        void shouldOmitWhenNoInclusions() {
            final var schemaBuilder = builder.omit("abc").schemaBuilder();
            final var outSchema =
                    schemaBuilder.buildOutboundSchema(
                            schema(Map.of("abc", inAbc, "def", inDef)), dependencyMap);
            assertThat(outSchema.field("def").field(), Matchers.sameInstance(inDef));
            assertThat(outSchema.size(), equalTo(1));
        }
    }

    @Nested
    class AliasTests {
        @Test
        void shouldAliasInboundField() {
            final var schemaBuilder =
                    builder.inboundAlias("abc", "foo").inboundAlias("def", "bar").schemaBuilder();
            final var outSchema =
                    schemaBuilder.buildOutboundSchema(
                            schema(Map.of("abc", inAbc, "def", inDef)), dependencyMap);
            assertThat(outSchema.field("foo").field(), Matchers.sameInstance(inAbc));
            assertThat(outSchema.field("bar").field(), Matchers.sameInstance(inDef));
            outSchema.field("foo").field().objectValueAt(0);
            outSchema.field("bar").field().objectValueAt(1);
            verify(inAbc, times(1)).objectValueAt(0);
            verify(inDef, times(1)).objectValueAt(1);
        }

        @Test
        void shouldNotIncludeAliasWhenOmitted() {
            final var schemaBuilder =
                    builder.omit("abc")
                            .inboundAlias("abc", "foo")
                            .inboundAlias("def", "bar")
                            .schemaBuilder();
            final var outSchema =
                    schemaBuilder.buildOutboundSchema(
                            schema(Map.of("abc", inAbc, "def", inDef)), dependencyMap);
            assertThat(outSchema.size(), equalTo(1));
            assertThrows(Exception.class, () -> outSchema.field("abc"));
        }
    }

    @Nested
    class OrderingTests {
        private Schema inSchema;

        @BeforeEach
        void setUp() {
            final Map<String, Field> fieldMap = new LinkedHashMap<>();
            IntStream.range(0, 10).forEach(i -> fieldMap.put("f" + i, field()));
            fieldMap.remove("f5"); // will replace with new field
            builder.lazyCalculation("f5", mock(IntFieldCalculation.class));
            inSchema = schema(fieldMap);
        }

        @Test
        void shouldOrderFromLeft() {
            final var schemaBuilder = builder.outboundOrderOnLeft("f3", "f4", "f5").schemaBuilder();
            final var outSchema = schemaBuilder.buildOutboundSchema(inSchema, dependencyMap);
            assertThat(fieldNames(outSchema).subList(0, 3), contains("f3", "f4", "f5"));
        }

        @Test
        void shouldOrderFromRight() {
            final var schemaBuilder =
                    builder.outboundOrderOnRight("f3", "f4", "f5").schemaBuilder();
            final var outSchema = schemaBuilder.buildOutboundSchema(inSchema, dependencyMap);
            assertThat(fieldNames(outSchema).subList(7, 10), contains("f3", "f4", "f5"));
        }

        @Test
        void shouldOrderFromLeftAndRight() {
            final var schemaBuilder =
                    builder.outboundOrderOnLeft("f3", "f4", "f5")
                            .outboundOrderOnRight("f1", "f2", "f7")
                            .schemaBuilder();
            final var outSchema = schemaBuilder.buildOutboundSchema(inSchema, dependencyMap);
            final var outNames = fieldNames(outSchema);
            assertThat(outNames.subList(0, 3), contains("f3", "f4", "f5"));
            assertThat(outNames.subList(7, 10), contains("f1", "f2", "f7"));
        }

        @Test
        void shouldThrowIfDuplicateNameInLeft() {
            assertThrows(
                    OperatorSetupException.class,
                    () -> builder.outboundOrderOnLeft("f3", "f3", "f5").schemaBuilder());
        }

        @Test
        void shouldThrowIfDuplicateNameInRight() {
            assertThrows(
                    OperatorSetupException.class,
                    () -> builder.outboundOrderOnRight("f3", "f3", "f5").schemaBuilder());
        }

        @Test
        void shouldThrowIfDuplicateNameInLeftAndRight() {
            assertThrows(
                    OperatorSetupException.class,
                    () ->
                            builder.outboundOrderOnLeft("f3", "f4", "f5")
                                    .outboundOrderOnRight("f4")
                                    .schemaBuilder());
        }
    }

    private List<String> fieldNames(final Schema schema) {
        final List<String> list = new ArrayList<>(schema.size());
        for (int i = 0, len = schema.size(); i < len; i++) {
            list.add(schema.fieldAt(i).name());
        }
        return list;
    }

    private Schema schema(final Map<String, Field> fields) {
        return Schema.schema("", fieldList(fields));
    }

    private Field field() {
        final var field = mock(IntField.class);
        lenient().when(field.typeId()).thenReturn(TypeId.Int);
        lenient().when(field.objectValueAt(anyInt())).thenReturn(0);
        return field;
    }
}
