// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.nats.kv;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import com.bytefacets.collections.types.BoolType;
import com.bytefacets.collections.types.ByteType;
import com.bytefacets.collections.types.CharType;
import com.bytefacets.collections.types.DoubleType;
import com.bytefacets.collections.types.FloatType;
import com.bytefacets.collections.types.IntType;
import com.bytefacets.collections.types.LongType;
import com.bytefacets.collections.types.ShortType;
import com.bytefacets.spinel.grpc.codec.ObjectDecoderRegistry;
import com.bytefacets.spinel.grpc.codec.ObjectEncoderImpl;
import com.bytefacets.spinel.grpc.codec.ObjectEncoderRegistry;
import com.bytefacets.spinel.schema.TypeId;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FieldReaderTest {
    private final byte[] data = new byte[256];
    private final ObjectEncoderImpl encoder = ObjectEncoderImpl.encoder();
    private final FieldReader reader = new FieldReader();
    private int pos = 2;

    @BeforeEach
    void setUp() {
        reader.set(2, data);
        final byte localDateType = 1;
        ObjectEncoderRegistry.register(
                LocalDate.class,
                (bufferSupplier, value) -> {
                    final ByteBuffer buffer = bufferSupplier.beginUserType(localDateType, 4);
                    buffer.putInt(IntType.fromLocalDate((LocalDate) value));
                });
        ObjectDecoderRegistry.register(
                localDateType,
                buffer -> {
                    final int yyyyMMdd = buffer.getInt();
                    return IntType.toLocalDate(yyyyMMdd);
                });
    }

    @Test
    void shouldReadBool() {
        writeBool(true);
        writeByte((byte) 0);
        writeByte((byte) 1);
        writeShort((short) 276);
        writeShort((short) 0);
        writeInt(0);
        writeInt(876837);
        final List<Boolean> observed = new ArrayList<>();
        while (reader.pos() < pos) {
            observed.add(reader.readToBool());
        }
        assertThat(observed, contains(true, false, true, true, false, false, true));
    }

    @Test
    void shouldReadByte() {
        writeBool(true);
        writeByte((byte) -87);
        writeByte((byte) 13);
        writeChar('A');
        final List<Byte> observed = new ArrayList<>();
        while (reader.pos() < pos) {
            observed.add(reader.readToByte());
        }
        assertThat(observed, contains((byte) 1, (byte) -87, (byte) 13, (byte) 65));
    }

    @Test
    void shouldReadShort() {
        writeBool(true);
        writeByte((byte) -87);
        writeByte((byte) 13);
        writeChar('A');
        writeShort((short) 8728);
        final List<Short> observed = new ArrayList<>();
        while (reader.pos() < pos) {
            observed.add(reader.readToShort());
        }
        assertThat(
                observed, contains((short) 1, (short) -87, (short) 13, (short) 65, (short) 8728));
    }

    @Test
    void shouldReadInt() {
        writeBool(true);
        writeByte((byte) 13);
        writeChar('A');
        writeShort((short) 8728);
        writeInt(3876387);
        final List<Integer> observed = new ArrayList<>();
        while (reader.pos() < pos) {
            observed.add(reader.readToInt());
        }
        assertThat(observed, contains(1, 13, 65, 8728, 3876387));
    }

    @Test
    void shouldReadLong() {
        writeBool(true);
        writeByte((byte) 13);
        writeChar('A');
        writeShort((short) 8728);
        writeInt(3876387);
        writeLong(-59872876542L);
        final List<Long> observed = new ArrayList<>();
        while (reader.pos() < pos) {
            observed.add(reader.readToLong());
        }
        assertThat(observed, contains(1L, 13L, 65L, 8728L, 3876387L, -59872876542L));
    }

    @Test
    void shouldReadFloat() {
        writeBool(true);
        writeByte((byte) 13);
        writeChar('A');
        writeShort((short) 8728);
        writeInt(3876387);
        writeFloat(-598728.76542f);
        final List<Float> observed = new ArrayList<>();
        while (reader.pos() < pos) {
            observed.add(reader.readToFloat());
        }
        assertThat(observed, contains(1f, 13f, 65f, 8728f, 3876387f, -598728.76542f));
    }

    @Test
    void shouldReadDouble() {
        writeBool(true);
        writeByte((byte) 13);
        writeChar('A');
        writeShort((short) 8728);
        writeInt(3876387);
        writeLong(88763876838783L);
        writeFloat(-5987f); // epsilon issues with other various examples
        writeDouble(872982.287872);
        final List<Double> observed = new ArrayList<>();
        while (reader.pos() < pos) {
            observed.add(reader.readToDouble());
        }
        assertThat(
                observed,
                contains(1d, 13d, 65d, 8728d, 3876387d, 88763876838783d, -5987d, 872982.287872d));
    }

    @Test
    void shouldReadString() {
        writeBool(true);
        writeByte((byte) 13);
        writeChar('A');
        writeShort((short) 8728);
        writeInt(3876387);
        writeLong(88763876838783L);
        writeFloat(-5987f); // epsilon issues with other various examples
        writeDouble(872982.287872);
        writeString(null);
        writeString("hello!");
        final List<String> observed = new ArrayList<>();
        while (reader.pos() < pos) {
            observed.add(reader.readToString());
        }
        assertThat(
                observed,
                contains(
                        "true",
                        "13",
                        "A",
                        "8728",
                        "3876387",
                        "88763876838783",
                        "-5987.0",
                        "872982.287872",
                        null,
                        "hello!"));
    }

    @Test
    void shouldReadGeneric() {
        writeBool(true);
        writeByte((byte) 13);
        writeChar('A');
        writeShort((short) 8728);
        writeInt(3876387);
        writeLong(88763876838783L);
        writeFloat(-5987f); // epsilon issues with other various examples
        writeDouble(872982.287872);
        writeString("hello!");
        writeGeneric(null);
        writeGeneric(LocalDate.of(2025, 9, 10));
        final List<Object> observed = new ArrayList<>();
        while (reader.pos() < pos) {
            observed.add(reader.readToGeneric());
        }
        assertThat(
                observed,
                contains(
                        Boolean.TRUE,
                        (byte) 13,
                        'A',
                        (short) 8728,
                        3876387,
                        88763876838783L,
                        -5987f,
                        872982.287872d,
                        "hello!",
                        null,
                        LocalDate.of(2025, 9, 10)));
    }

    private void writeBool(final boolean value) {
        data[pos++] = TypeId.Bool;
        pos = BoolType.writeLE(data, pos, value);
    }

    private void writeByte(final byte value) {
        data[pos++] = TypeId.Byte;
        pos = ByteType.writeLE(data, pos, value);
    }

    private void writeShort(final short value) {
        data[pos++] = TypeId.Short;
        pos = ShortType.writeLE(data, pos, value);
    }

    private void writeChar(final char value) {
        data[pos++] = TypeId.Char;
        pos = CharType.writeLE(data, pos, value);
    }

    private void writeInt(final int value) {
        data[pos++] = TypeId.Int;
        pos = IntType.writeLE(data, pos, value);
    }

    private void writeLong(final long value) {
        data[pos++] = TypeId.Long;
        pos = LongType.writeLE(data, pos, value);
    }

    private void writeFloat(final float value) {
        data[pos++] = TypeId.Float;
        pos = FloatType.writeLE(data, pos, value);
    }

    private void writeDouble(final double value) {
        data[pos++] = TypeId.Double;
        pos = DoubleType.writeLE(data, pos, value);
    }

    private void writeString(final String value) {
        data[pos++] = TypeId.String;
        if (value != null) {
            final byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
            pos = IntType.writeLE(data, pos, valueBytes.length);
            System.arraycopy(valueBytes, 0, data, pos, valueBytes.length);
            pos += valueBytes.length;
        } else {
            pos = IntType.writeLE(data, pos, -1);
        }
    }

    private void writeGeneric(final Object value) {
        data[pos++] = TypeId.Generic;
        final byte[] valueBytes = encoder.encodeToArray(value);
        pos = IntType.writeLE(data, pos, valueBytes.length);
        System.arraycopy(valueBytes, 0, data, pos, valueBytes.length);
        pos += valueBytes.length;
    }
}
