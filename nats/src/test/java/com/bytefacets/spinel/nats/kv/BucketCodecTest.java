// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.nats.kv;

import static com.bytefacets.collections.types.ByteType.castToByte;
import static com.bytefacets.collections.types.CharType.castToChar;
import static com.bytefacets.collections.types.DoubleType.castToDouble;
import static com.bytefacets.collections.types.FloatType.castToFloat;
import static com.bytefacets.collections.types.GenericType.castToGeneric;
import static com.bytefacets.collections.types.IntType.castToInt;
import static com.bytefacets.collections.types.LongType.castToLong;
import static com.bytefacets.collections.types.ShortType.castToShort;
import static com.bytefacets.collections.types.StringType.castToString;
import static com.bytefacets.spinel.grpc.receive.SchemaBuilder.schemaBuilder;
import static com.bytefacets.spinel.schema.ArrayFieldFactory.writableArrayField;
import static com.bytefacets.spinel.schema.FieldList.fieldList;
import static com.bytefacets.spinel.schema.MatrixStoreFieldFactory.matrixStoreFieldFactory;
import static com.bytefacets.spinel.schema.SchemaField.schemaField;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

import com.bytefacets.collections.hash.StringGenericIndexedMap;
import com.bytefacets.spinel.grpc.proto.FieldDefinition;
import com.bytefacets.spinel.grpc.proto.SchemaUpdate;
import com.bytefacets.spinel.schema.BoolWritableField;
import com.bytefacets.spinel.schema.ByteWritableField;
import com.bytefacets.spinel.schema.CharWritableField;
import com.bytefacets.spinel.schema.DoubleWritableField;
import com.bytefacets.spinel.schema.FloatWritableField;
import com.bytefacets.spinel.schema.GenericWritableField;
import com.bytefacets.spinel.schema.IntWritableField;
import com.bytefacets.spinel.schema.LongWritableField;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.SchemaField;
import com.bytefacets.spinel.schema.ShortWritableField;
import com.bytefacets.spinel.schema.StringWritableField;
import com.bytefacets.spinel.schema.TypeId;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@SuppressWarnings("CyclomaticComplexity")
@ExtendWith(MockitoExtension.class)
final class BucketCodecTest {
    private static final int[] ALL_TYPES_IN_ORDER =
            IntStream.rangeClosed(TypeId.Min, TypeId.Max).toArray();
    private @Mock SchemaRegistry schemaRegistry;
    private final BucketEncoder encoder = new BucketEncoder();
    private final BitSet changedFields = new BitSet();
    private BucketDecoder decoder;

    @BeforeEach
    void setUp() {
        decoder =
                new BucketDecoder(
                        changedFields,
                        schemaBuilder(matrixStoreFieldFactory(4, 4, i -> {})),
                        schemaRegistry);
    }

    @Nested
    class SchemaBindingTests {
        @Test
        void shouldBuildSchemaFromSchemaUpdateMessage() {
            final Schema schema = decoder.setSchema(5, schemaMsg(ALL_TYPES_IN_ORDER));
            for (int i = 0, len = schema.size(); i < len; i++) {
                final byte typeId = (byte) (i + 1);
                assertThat(schema.fieldAt(i).typeId(), equalTo(typeId));
                assertThat(schema.fieldAt(i).name(), equalTo("f" + TypeId.toTypeName(typeId)));
            }
            assertThat(schema.size(), equalTo(TypeId.Max - TypeId.Min + 1));
            assertThat(schema.name(), equalTo("TEST"));
        }
    }

    @Nested
    class SameSchemaDataWritingTests {
        private static final int schemaId = 6;
        Schema targetSchema;
        Schema sourceSchema;

        @BeforeEach
        void setUp() {
            targetSchema = decoder.setSchema(schemaId, schemaMsg(ALL_TYPES_IN_ORDER));
            sourceSchema = sourceSchema(ALL_TYPES_IN_ORDER);
        }

        @Test
        void shouldRoundTripRow() {
            final int sourceRow = 5;
            final int targetRow = 2;
            encoder.setSchema(schemaId, sourceSchema);
            write(sourceSchema, sourceRow, 7);
            final byte[] rowData = encoder.encode(sourceRow);
            decoder.write(true, targetRow, rowData);
            assertValues(targetSchema, targetRow, 7, Set.of());
            assertThat(changedFields.cardinality(), equalTo(0));
        }

        @Test
        void shouldFlagChangedFieldsOnUpdateAll() {
            final int sourceRow = 5;
            final int targetRow = 2;
            encoder.setSchema(schemaId, sourceSchema);
            write(sourceSchema, sourceRow, 7);
            final byte[] addData = encoder.encode(sourceRow);
            decoder.write(true, targetRow, addData);

            write(sourceSchema, sourceRow, 8);
            write(sourceSchema.field("fBool"), sourceRow, 0);
            final BitSet expected = new BitSet();
            expected.set(0, targetSchema.size());
            final byte[] updateData = encoder.encode(sourceRow);
            decoder.write(false, targetRow, updateData);
            assertThat(changedFields, equalTo(expected));
            final Set<String> assertValuesFields = new HashSet<>();
            targetSchema.forEachField(f -> assertValuesFields.add(f.name()));
            assertValuesFields.remove("fBool");
            assertValues(targetSchema, targetRow, 8, assertValuesFields);
        }

        @Test
        void shouldFlagChangedFieldsOnUpdateSelected() {
            final int sourceRow = 5;
            final int targetRow = 2;
            encoder.setSchema(schemaId, sourceSchema);
            write(sourceSchema, sourceRow, 7);
            final byte[] addData = encoder.encode(sourceRow);
            decoder.write(true, targetRow, addData);

            final BitSet expected = new BitSet();
            Stream.of("fInt", "fDouble")
                    .forEach(
                            name -> {
                                write(sourceSchema.field(name), sourceRow, 8);
                                expected.set(targetSchema.field(name).fieldId());
                            });
            final byte[] updateData = encoder.encode(sourceRow);
            decoder.write(false, targetRow, updateData);
            assertThat(changedFields, equalTo(expected));
            assertValues(targetSchema, targetRow, 8, Set.of("fInt", "fDouble"));
        }
    }

    @Nested
    class OtherSchemaDataWritingTests {
        private static final int targetSchemaId = 6;
        private static final int sourceRow = 5;
        private static final int targetRow = 2;
        Schema targetSchema;
        Schema sourceSchema;

        @BeforeEach
        void setUp() {
            targetSchema = decoder.setSchema(targetSchemaId, schemaMsg(ALL_TYPES_IN_ORDER));
        }

        @Test
        void shouldReadOtherSchemaWithSameNamesInDifferentOrder() {
            final int[] sourceTypeIdOrder = new int[] {5, 4, 10, 6, 3, 9, 1, 2, 7, 8};
            sourceSchema = sourceSchema(sourceTypeIdOrder);
            when(schemaRegistry.lookup(777)).thenReturn(schemaMsg(sourceTypeIdOrder));
            encoder.setSchema(777, sourceSchema);

            write(sourceSchema, sourceRow, 9);
            final byte[] rowData = encoder.encode(sourceRow);
            decoder.write(true, targetRow, rowData);
            assertValues(targetSchema, targetRow, 9, Set.of());
        }

        @Test
        void shouldReadOtherSchemaWithExtraFields() {
            final int[] sourceTypeIdOrder = new int[] {5, 4, 10, 6, 6, 7, 4, 5, 3, 9, 1, 2, 7, 8};
            sourceSchema = sourceSchema(sourceTypeIdOrder);
            when(schemaRegistry.lookup(777)).thenReturn(schemaMsg(sourceTypeIdOrder));
            encoder.setSchema(777, sourceSchema);

            write(sourceSchema, sourceRow, 9);
            final byte[] rowData = encoder.encode(sourceRow);
            decoder.write(true, targetRow, rowData);
            assertValues(targetSchema, targetRow, 9, Set.of());
        }

        @Test
        void shouldReadOtherSchemaWithFewerFields() {
            final int[] sourceTypeIdOrder = new int[] {2, 10, 9, 4};
            sourceSchema = sourceSchema(sourceTypeIdOrder);
            when(schemaRegistry.lookup(777)).thenReturn(schemaMsg(sourceTypeIdOrder));
            encoder.setSchema(777, sourceSchema);

            write(sourceSchema, sourceRow, 9);
            final byte[] rowData = encoder.encode(sourceRow);
            decoder.write(true, targetRow, rowData);
            assertValues(
                    targetSchema, targetRow, 9, Set.of("fByte", "fGeneric", "fString", "fChar"));
        }
    }

    private void assertValues(
            final Schema schema, final int row, final int salt, final Set<String> filterNames) {
        schema.forEachField(
                sField -> {
                    if (!filterNames.isEmpty() && !filterNames.contains(sField.name())) {
                        return;
                    }
                    final var field = sField.field();
                    switch (field.typeId()) {
                        case TypeId.Bool -> assertThat(field.objectValueAt(row), equalTo(true));
                        case TypeId.Byte ->
                                assertThat(field.objectValueAt(row), equalTo(castToByte(salt * 2)));
                        case TypeId.Short ->
                                assertThat(
                                        field.objectValueAt(row), equalTo(castToShort(salt * 3)));
                        case TypeId.Char ->
                                assertThat(field.objectValueAt(row), equalTo(castToChar(salt * 4)));
                        case TypeId.Int ->
                                assertThat(field.objectValueAt(row), equalTo(castToInt(salt * 5)));
                        case TypeId.Long ->
                                assertThat(field.objectValueAt(row), equalTo(castToLong(salt * 6)));
                        case TypeId.Float ->
                                assertThat(
                                        field.objectValueAt(row), equalTo(castToFloat(salt * 7)));
                        case TypeId.Double ->
                                assertThat(
                                        field.objectValueAt(row), equalTo(castToDouble(salt * 8)));
                        case TypeId.String ->
                                assertThat(
                                        field.objectValueAt(row), equalTo(castToString(salt * 9)));
                        case TypeId.Generic ->
                                assertThat(
                                        field.objectValueAt(row),
                                        equalTo(castToGeneric(salt * 10)));
                        default -> throw new RuntimeException("unhandled type " + field.typeId());
                    }
                });
    }

    private void write(final Schema schema, final int row, final int salt) {
        schema.forEachField(sField -> write(sField, row, salt));
    }

    private void write(final SchemaField sField, final int row, final int salt) {
        final var field = sField.field();
        switch (field.typeId()) {
            case TypeId.Bool -> ((BoolWritableField) field).setValueAt(row, salt != 0);
            case TypeId.Byte -> ((ByteWritableField) field).setValueAt(row, castToByte(salt * 2));
            case TypeId.Short ->
                    ((ShortWritableField) field).setValueAt(row, castToShort(salt * 3));
            case TypeId.Char -> ((CharWritableField) field).setValueAt(row, castToChar(salt * 4));
            case TypeId.Int -> ((IntWritableField) field).setValueAt(row, castToInt(salt * 5));
            case TypeId.Long -> ((LongWritableField) field).setValueAt(row, castToLong(salt * 6));
            case TypeId.Float ->
                    ((FloatWritableField) field).setValueAt(row, castToFloat(salt * 7));
            case TypeId.Double ->
                    ((DoubleWritableField) field).setValueAt(row, castToDouble(salt * 8));
            case TypeId.String ->
                    ((StringWritableField) field).setValueAt(row, castToString(salt * 9));
            case TypeId.Generic ->
                    ((GenericWritableField) field).setValueAt(row, castToGeneric(salt * 10));
            default -> throw new RuntimeException("unhandled type " + field.typeId());
        }
    }

    private SchemaUpdate schemaMsg(final int[] types) {
        return schemaMsg(SchemaUpdate.newBuilder(), types);
    }

    private SchemaUpdate schemaMsg(final SchemaUpdate.Builder builder, final int[] types) {
        final NameSelector nameSelector = new NameSelector();
        builder.setName("TEST");
        IntStream.of(types).mapToObj(type -> field(type, nameSelector)).forEach(builder::addFields);
        return builder.build();
    }

    private FieldDefinition field(final int typeId, final NameSelector usedNames) {
        return FieldDefinition.newBuilder()
                .setName(usedNames.createName(typeId))
                .setTypeId(typeId)
                .build();
    }

    private Schema sourceSchema(final int[] types) {
        final StringGenericIndexedMap<SchemaField> fieldMap = new StringGenericIndexedMap<>(16);
        final NameSelector nameSelector = new NameSelector();
        IntStream.of(types)
                .mapToObj(type -> writableArrayField((byte) type, 4, type - 1, i -> {}))
                .forEach(
                        field -> {
                            final String name = nameSelector.createName(field.typeId());
                            final int fieldId = fieldMap.add(name);
                            fieldMap.putValueAt(fieldId, schemaField(fieldId, name, field));
                        });
        return Schema.schema("TEST", fieldList(fieldMap));
    }

    private static class NameSelector {
        private final Set<String> used = new HashSet<>();

        String createName(final int type) {
            int refCt = 0;
            final String rootName = "f" + TypeId.toTypeName((byte) type);
            String name = rootName;
            while (used.contains(name)) {
                refCt++;
                name = rootName + "_" + refCt;
            }
            used.add(name);
            return name;
        }
    }
}
