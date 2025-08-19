// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.schema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CastTest {
    private final BoolWritableField boolField =
            ArrayFieldFactory.writableBoolArrayField(3, 0, i -> {});
    private final ByteWritableField byteField =
            ArrayFieldFactory.writableByteArrayField(3, 0, i -> {});
    private final ShortWritableField shortField =
            ArrayFieldFactory.writableShortArrayField(3, 0, i -> {});
    private final CharWritableField charField =
            ArrayFieldFactory.writableCharArrayField(3, 0, i -> {});
    private final IntWritableField intField =
            ArrayFieldFactory.writableIntArrayField(3, 0, i -> {});
    private final LongWritableField longField =
            ArrayFieldFactory.writableLongArrayField(3, 0, i -> {});
    private final FloatWritableField floatField =
            ArrayFieldFactory.writableFloatArrayField(3, 0, i -> {});
    private final DoubleWritableField doubleField =
            ArrayFieldFactory.writableDoubleArrayField(3, 0, i -> {});
    private final StringWritableField stringField =
            ArrayFieldFactory.writableStringArrayField(3, 0, i -> {});
    private final GenericWritableField genericField =
            ArrayFieldFactory.writableGenericArrayField(3, 0, i -> {});

    @BeforeEach
    void setUp() {
        boolField.setValueAt(2, true);
        byteField.setValueAt(2, (byte) 32);
        shortField.setValueAt(2, (short) 32);
        charField.setValueAt(2, (char) 32);
        intField.setValueAt(2, (char) 32);
        longField.setValueAt(2, (char) 32);
        floatField.setValueAt(2, (char) 32);
        doubleField.setValueAt(2, (char) 32);
        stringField.setValueAt(2, "32");
        genericField.setValueAt(2, 32);
    }

    @Test
    void shouldCastToBool() {
        Stream.of(boolField, byteField, shortField, charField, intField)
                .forEach(field -> assertThat(Cast.toBoolField(field).valueAt(2), equalTo(true)));
    }

    @Test
    void shouldCastToByte() {
        Stream.of(byteField, charField)
                .forEach(
                        field ->
                                assertThat(Cast.toByteField(field).valueAt(2), equalTo((byte) 32)));
        assertThat(Cast.toByteField(boolField).valueAt(2), equalTo((byte) 1));
    }

    @Test
    void shouldCastToShort() {
        Stream.of(byteField, charField, shortField)
                .forEach(
                        field ->
                                assertThat(
                                        Cast.toShortField(field).valueAt(2), equalTo((short) 32)));
        assertThat(Cast.toShortField(boolField).valueAt(2), equalTo((short) 1));
    }

    @Test
    void shouldCastToChar() {
        Stream.of(byteField, charField, shortField)
                .forEach(
                        field ->
                                assertThat(Cast.toCharField(field).valueAt(2), equalTo((char) 32)));
        assertThat(Cast.toCharField(stringField).valueAt(2), equalTo('3'));
        assertThat(Cast.toCharField(boolField).valueAt(2), equalTo('T'));
    }

    @Test
    void shouldCastToInt() {
        Stream.of(byteField, charField, shortField, intField)
                .forEach(field -> assertThat(Cast.toIntField(field).valueAt(2), equalTo(32)));
        assertThat(Cast.toIntField(boolField).valueAt(2), equalTo(1));
    }

    @Test
    void shouldCastToLong() {
        Stream.of(byteField, charField, shortField, intField, longField)
                .forEach(field -> assertThat(Cast.toLongField(field).valueAt(2), equalTo(32L)));
        assertThat(Cast.toLongField(boolField).valueAt(2), equalTo(1L));
    }

    @Test
    void shouldCastToFloat() {
        Stream.of(byteField, shortField, intField, floatField)
                .forEach(field -> assertThat(Cast.toFloatField(field).valueAt(2), equalTo(32f)));
        assertThat(Cast.toFloatField(boolField).valueAt(2), equalTo(1f));
    }

    @Test
    void shouldCastToDouble() {
        Stream.of(byteField, shortField, intField, floatField, doubleField)
                .forEach(field -> assertThat(Cast.toDoubleField(field).valueAt(2), equalTo(32d)));
        assertThat(Cast.toDoubleField(boolField).valueAt(2), equalTo(1d));
    }

    @Test
    void shouldCastToString() {
        Stream.of(byteField, shortField, intField, stringField)
                .forEach(field -> assertThat(Cast.toStringField(field).valueAt(2), equalTo("32")));
        assertThat(Cast.toStringField(floatField).valueAt(2), equalTo("32.0"));
        assertThat(Cast.toStringField(doubleField).valueAt(2), equalTo("32.0"));
        assertThat(Cast.toStringField(charField).valueAt(2), equalTo(" "));
        assertThat(Cast.toStringField(boolField).valueAt(2), equalTo("true"));
    }

    @Test
    void shouldCastToGeneric() {
        assertThat(Cast.toGenericField(genericField).valueAt(2), equalTo(32));
        assertThat(Cast.toGenericField(stringField).valueAt(2), equalTo("32"));
        assertThat(Cast.toGenericField(intField).valueAt(2), equalTo(32));
        assertThat(Cast.toGenericField(byteField).valueAt(2), equalTo((byte) 32));
        assertThat(Cast.toGenericField(shortField).valueAt(2), equalTo((short) 32));
        assertThat(Cast.toGenericField(charField).valueAt(2), equalTo(' '));
        assertThat(Cast.toGenericField(floatField).valueAt(2), equalTo(32f));
        assertThat(Cast.toGenericField(doubleField).valueAt(2), equalTo(32d));
        assertThat(Cast.toGenericField(boolField).valueAt(2), equalTo(true));
    }
}
