// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.join;

import static com.bytefacets.diaspore.join.JoinSchemaBuilder.schemaResources;
import static com.bytefacets.diaspore.schema.FieldBitSet.fieldBitSet;
import static com.bytefacets.diaspore.schema.FieldList.fieldList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bytefacets.collections.functional.IntConsumer;
import com.bytefacets.collections.hash.StringGenericIndexedMap;
import com.bytefacets.diaspore.common.NameConflictResolver;
import com.bytefacets.diaspore.interner.RowInterner;
import com.bytefacets.diaspore.schema.FieldBitSet;
import com.bytefacets.diaspore.schema.FieldResolver;
import com.bytefacets.diaspore.schema.IntField;
import com.bytefacets.diaspore.schema.RowMapper;
import com.bytefacets.diaspore.schema.Schema;
import com.bytefacets.diaspore.schema.SchemaField;
import com.bytefacets.diaspore.schema.TypeId;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JoinSchemaBuilderTest {
    private final NameConflictResolver resolver = new NameConflictResolver() {};
    private @Mock(lenient = true) JoinMapper mapper;
    private @Mock(lenient = true) JoinInterner interner;
    private @Mock(lenient = true) RowInterner leftRowInterner;
    private @Mock(lenient = true) RowInterner rightRowInterner;
    private @Mock(lenient = true) RowMapper leftRowMapper;
    private @Mock(lenient = true) RowMapper rightRowMapper;

    @BeforeEach
    void setUp() {
        when(interner.left()).thenReturn(leftRowInterner);
        when(interner.right()).thenReturn(rightRowInterner);
        when(mapper.leftMapper()).thenReturn(leftRowMapper);
        when(mapper.rightMapper()).thenReturn(rightRowMapper);
    }

    @Test
    void shouldBindSchemasToInterner() {
        final var builder = builder("foo");
        builder.buildSchema(leftSchemaInput("A", "B"), rightSchemaInput("C", "D"));
        doAnswer(
                        inv -> {
                            final FieldResolver left = inv.getArgument(0);
                            Stream.of("A", "B").forEach(left::getField);
                            final FieldResolver right = inv.getArgument(1);
                            Stream.of("C", "D").forEach(right::getField);
                            return null;
                        })
                .when(interner)
                .bindToSchemas(any(), any());
        verify(interner, times(1)).bindToSchemas(any(), any());
    }

    @Test
    void shouldBuildSchema() {
        final var builder = builder("foo");
        final var outSchema =
                builder.buildSchema(leftSchemaInput("A", "B"), rightSchemaInput("C", "D"));
        assertThat(outSchema.size(), equalTo(4));
        assertThat(outSchema.name(), equalTo("foo"));
        final List<String> names = new ArrayList<>();
        outSchema.forEachField(field -> names.add(field.name()));
        assertThat(names, contains("A", "B", "C", "D"));
    }

    @Test
    void shouldIncludeLeftSourceRowField() {
        final var builder = builder("foo", "left-source-row", null);
        final var outSchema =
                builder.buildSchema(leftSchemaInput("A", "B"), rightSchemaInput("C", "D"));
        assertThat(outSchema.size(), equalTo(5));
        final List<String> names = new ArrayList<>();
        outSchema.forEachField(field -> names.add(field.name()));
        assertThat(names, contains("left-source-row", "A", "B", "C", "D"));
        outSchema.field("left-source-row").field().objectValueAt(10);
        verify(leftRowMapper, times(1)).sourceRowOf(10);
    }

    @Test
    void shouldIncludeRightSourceRowField() {
        final var builder = builder("foo", null, "right-source-row");
        final var outSchema =
                builder.buildSchema(leftSchemaInput("A", "B"), rightSchemaInput("C", "D"));
        assertThat(outSchema.size(), equalTo(5));
        final List<String> names = new ArrayList<>();
        outSchema.forEachField(field -> names.add(field.name()));
        assertThat(names, contains("right-source-row", "A", "B", "C", "D"));
        outSchema.field("right-source-row").field().objectValueAt(10);
        verify(rightRowMapper, times(1)).sourceRowOf(10);
    }

    @Nested
    class FieldMappingTests {
        private @Mock IntConsumer intConsumer;
        private JoinSchemaBuilder builder;
        private JoinSchemaBuilder.SchemaResources leftSchemaInput;
        private JoinSchemaBuilder.SchemaResources rightSchemaInput;
        private Schema outSchema;
        private final FieldBitSet inChanges = fieldBitSet();
        private @Captor ArgumentCaptor<Integer> idCaptor;

        @BeforeEach
        void setUp() {
            builder = builder("foo", "left-row", "right-row");
            leftSchemaInput = leftSchemaInput("A", "B", "C");
            rightSchemaInput = rightSchemaInput("B", "C", "D");
            outSchema = builder.buildSchema(leftSchemaInput, rightSchemaInput);
        }

        @Test
        void shouldMapLeftFields() {
            final var fieldMapping = leftSchemaInput.buildFieldMapping();
            leftSchemaInput.schema().forEachField(f -> inChanges.fieldChanged(f.fieldId()));
            fieldMapping.translateInboundChangeSet(inChanges, intConsumer);
            verify(intConsumer, times(3)).accept(idCaptor.capture());
            assertThat(
                    idCaptor.getAllValues(),
                    contains(
                            outSchema.field("A").fieldId(),
                            outSchema.field("B").fieldId(),
                            outSchema.field("C").fieldId()));
        }

        @Test
        void shouldMapRightFields() {
            final var fieldMapping = rightSchemaInput.buildFieldMapping();
            rightSchemaInput.schema().forEachField(f -> inChanges.fieldChanged(f.fieldId()));
            fieldMapping.translateInboundChangeSet(inChanges, intConsumer);
            verify(intConsumer, times(3)).accept(idCaptor.capture());
            assertThat(
                    idCaptor.getAllValues(),
                    contains(
                            outSchema.field("B_1").fieldId(),
                            outSchema.field("C_1").fieldId(),
                            outSchema.field("D").fieldId()));
        }
    }

    @Test
    void shouldResolveNameConflicts() {
        final var builder = builder("foo");
        final var outSchema =
                builder.buildSchema(
                        leftSchemaInput("A", "B", "B_1", "C"),
                        rightSchemaInput("B", "B_1", "C", "D"));
        final List<String> names = new ArrayList<>();
        outSchema.forEachField(field -> names.add(field.name()));
        assertThat(names, contains("A", "B", "B_1", "C", "B_2", "B_1_1", "C_1", "D"));
    }

    @Test
    void shouldDropNameConflictsWhenResolverReturnsNull() {
        final var mockResolver = mock(NameConflictResolver.class);
        when(mockResolver.resolveNameConflict(any(), any())).thenReturn(null);
        final var builder =
                new JoinSchemaBuilder(
                        "foo", null, null, mapper, interner, mockResolver, JoinKeyHandling.KeepAll);
        final var leftSchemaInput = leftSchemaInput("A", "B", "C");
        final var rightSchemaInput = rightSchemaInput("B", "C", "D");
        final var outSchema = builder.buildSchema(leftSchemaInput, rightSchemaInput);
        final List<String> names = new ArrayList<>();
        outSchema.forEachField(field -> names.add(field.name()));
        assertThat(names, contains("A", "B", "C", "D"));
        when(leftRowMapper.sourceRowOf(10)).thenReturn(7);
        when(rightRowMapper.sourceRowOf(10)).thenReturn(5);
        Stream.of("A", "B", "C")
                .forEach(
                        leftName -> {
                            outSchema.field(leftName).objectValueAt(10);
                            final var srcField = leftSchemaInput.schema().field(leftName);
                            verify(((IntField) srcField.field()), times(1)).valueAt(7);
                        });
        Stream.of("D")
                .forEach(
                        rightName -> {
                            outSchema.field(rightName).objectValueAt(10);
                            final var srcField = rightSchemaInput.schema().field(rightName);
                            verify(((IntField) srcField.field()), times(1)).valueAt(5);
                        });
    }

    @ParameterizedTest
    @EnumSource(JoinKeyHandling.class)
    void shouldDropJoinKeys(final JoinKeyHandling handling) {
        doAnswer(
                        inv -> {
                            final FieldResolver left = inv.getArgument(0);
                            Stream.of("B-l", "C-l").forEach(left::getField);
                            final FieldResolver right = inv.getArgument(1);
                            Stream.of("B-r", "C-r").forEach(right::getField);
                            return null;
                        })
                .when(interner)
                .bindToSchemas(any(), any());
        final var builder =
                new JoinSchemaBuilder("foo", null, null, mapper, interner, resolver, handling);
        final var leftSchemaInput = leftSchemaInput("A", "B-l", "C-l");
        final var rightSchemaInput = rightSchemaInput("B-r", "C-r", "D");
        final var outSchema = builder.buildSchema(leftSchemaInput, rightSchemaInput);
        final List<String> outFields = new ArrayList<>();
        outSchema.forEachField(f -> outFields.add(f.name()));
        if (handling.equals(JoinKeyHandling.KeepLeft)) {
            assertThat(outFields, contains("A", "B-l", "C-l", "D"));
        } else if (handling.equals(JoinKeyHandling.KeepRight)) {
            assertThat(outFields, contains("A", "B-r", "C-r", "D"));
        } else if (handling.equals(JoinKeyHandling.KeepAll)) {
            assertThat(outFields, contains("A", "B-l", "C-l", "B-r", "C-r", "D"));
        } else {
            assertThat(outFields, contains("A", "D"));
        }
    }

    private JoinSchemaBuilder builder(final String name) {
        return builder(name, null, null);
    }

    private JoinSchemaBuilder builder(
            final String name, final String leftSourceRow, final String rightSourceRow) {
        return new JoinSchemaBuilder(
                name,
                leftSourceRow,
                rightSourceRow,
                mapper,
                interner,
                resolver,
                JoinKeyHandling.KeepAll);
    }

    private Schema schema(final String schemaName, final String... names) {
        final StringGenericIndexedMap<SchemaField> fieldMap = new StringGenericIndexedMap<>(10);
        Stream.of(names)
                .forEach(
                        name -> {
                            final int id = fieldMap.add(name);
                            fieldMap.putValueAt(id, SchemaField.schemaField(id, name, field()));
                        });
        return Schema.schema(schemaName, fieldList(fieldMap));
    }

    private JoinSchemaBuilder.SchemaResources leftSchemaInput(final String... names) {
        return schemaResources(schema("left", names), true);
    }

    private JoinSchemaBuilder.SchemaResources rightSchemaInput(final String... names) {
        return schemaResources(schema("right", names), false);
    }

    private IntField field() {
        final IntField field = mock(IntField.class);
        lenient().when(field.typeId()).thenReturn(TypeId.Int);
        return field;
    }
}
