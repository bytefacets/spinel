// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.nats.kv;

import com.bytefacets.collections.types.CharType;
import com.bytefacets.collections.types.DoubleType;
import com.bytefacets.collections.types.FloatType;
import com.bytefacets.collections.types.IntType;
import com.bytefacets.collections.types.LongType;
import com.bytefacets.collections.types.ShortType;
import com.bytefacets.spinel.grpc.codec.ObjectDecoderRegistry;
import com.bytefacets.spinel.schema.TypeId;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;

@SuppressWarnings("CyclomaticComplexity")
final class FieldReader {
    private int pos = -1;
    private byte[] data;

    FieldReader() {}

    void set(final int pos, final byte[] data) {
        this.pos = pos;
        this.data = data;
    }

    int pos() {
        return pos;
    }

    boolean more() {
        return pos < data.length;
    }

    boolean readToBool() {
        final byte typeId = data[pos++];
        return switch (typeId) {
            case TypeId.Bool -> internalReadBool();
            case TypeId.Byte -> internalReadByte() != 0;
            case TypeId.Short -> internalReadShort() != 0;
            case TypeId.Int -> internalReadInt() != 0;
            default -> throw cannotCast(TypeId.Bool, typeId, pos - 1);
        };
    }

    byte readToByte() {
        final byte typeId = data[pos++];
        return switch (typeId) {
            case TypeId.Bool -> (byte) (internalReadBool() ? 1 : 0);
            case TypeId.Byte -> internalReadByte();
            case TypeId.Char -> (byte) (internalReadChar());
            default -> throw cannotCast(TypeId.Byte, typeId, pos - 1);
        };
    }

    short readToShort() {
        final byte typeId = data[pos++];
        return switch (typeId) {
            case TypeId.Bool -> (short) (internalReadBool() ? 1 : 0);
            case TypeId.Byte -> internalReadByte();
            case TypeId.Char -> (short) (internalReadChar());
            case TypeId.Short -> internalReadShort();
            default -> throw cannotCast(TypeId.Short, typeId, pos - 1);
        };
    }

    char readToChar() {
        final byte typeId = data[pos++];
        return switch (typeId) {
            case TypeId.Bool -> internalReadBool() ? 'T' : 'F';
            case TypeId.Byte -> (char) internalReadByte();
            case TypeId.Char -> internalReadChar();
            case TypeId.Short -> (char) internalReadShort();
            default -> throw cannotCast(TypeId.Char, typeId, pos - 1);
        };
    }

    int readToInt() {
        final byte typeId = data[pos++];
        return switch (typeId) {
            case TypeId.Bool -> (internalReadBool() ? 1 : 0);
            case TypeId.Byte -> internalReadByte();
            case TypeId.Char -> internalReadChar();
            case TypeId.Short -> internalReadShort();
            case TypeId.Int -> internalReadInt();
            default -> throw cannotCast(TypeId.Int, typeId, pos - 1);
        };
    }

    long readToLong() {
        final byte typeId = data[pos++];
        return switch (typeId) {
            case TypeId.Bool -> (internalReadBool() ? 1 : 0);
            case TypeId.Byte -> internalReadByte();
            case TypeId.Char -> internalReadChar();
            case TypeId.Short -> internalReadShort();
            case TypeId.Int -> internalReadInt();
            case TypeId.Long -> internalReadLong();
            default -> throw cannotCast(TypeId.Long, typeId, pos - 1);
        };
    }

    float readToFloat() {
        final byte typeId = data[pos++];
        return switch (typeId) {
            case TypeId.Bool -> (internalReadBool() ? 1 : 0);
            case TypeId.Byte -> internalReadByte();
            case TypeId.Char -> internalReadChar();
            case TypeId.Short -> internalReadShort();
            case TypeId.Int -> internalReadInt();
            case TypeId.Float -> internalReadFloat();
            default -> throw cannotCast(TypeId.Float, typeId, pos - 1);
        };
    }

    double readToDouble() {
        final byte typeId = data[pos++];
        return switch (typeId) {
            case TypeId.Bool -> (internalReadBool() ? 1 : 0);
            case TypeId.Byte -> internalReadByte();
            case TypeId.Char -> internalReadChar();
            case TypeId.Short -> internalReadShort();
            case TypeId.Int -> internalReadInt();
            case TypeId.Long -> internalReadLong();
            case TypeId.Float -> internalReadFloat();
            case TypeId.Double -> internalReadDouble();
            default -> throw cannotCast(TypeId.Double, typeId, pos - 1);
        };
    }

    String readToString() {
        final byte typeId = data[pos++];
        return switch (typeId) {
            case TypeId.Bool -> Boolean.toString(internalReadBool());
            case TypeId.Byte -> Byte.toString(internalReadByte());
            case TypeId.Char -> Character.toString(internalReadChar());
            case TypeId.Short -> Short.toString(internalReadShort());
            case TypeId.Int -> Integer.toString(internalReadInt());
            case TypeId.Long -> Long.toString(internalReadLong());
            case TypeId.Float -> Float.toString(internalReadFloat());
            case TypeId.Double -> Double.toString(internalReadDouble());
            case TypeId.String -> internalReadString();
            default -> throw cannotCast(TypeId.Double, typeId, pos - 1);
        };
    }

    Object readToGeneric() {
        final byte typeId = data[pos++];
        return switch (typeId) {
            case TypeId.Bool -> internalReadBool();
            case TypeId.Byte -> internalReadByte();
            case TypeId.Char -> internalReadChar();
            case TypeId.Short -> internalReadShort();
            case TypeId.Int -> internalReadInt();
            case TypeId.Long -> internalReadLong();
            case TypeId.Float -> internalReadFloat();
            case TypeId.Double -> internalReadDouble();
            case TypeId.String -> internalReadString();
            case TypeId.Generic -> internalReadGeneric();
            default -> throw cannotCast(TypeId.Generic, typeId, pos - 1);
        };
    }

    @SuppressWarnings("InnerAssignment")
    void skip() {
        final byte typeId = data[pos++];
        switch (typeId) {
            case TypeId.Bool, TypeId.Byte -> pos += 1;
            case TypeId.Char, TypeId.Short -> pos += 2;
            case TypeId.Int, TypeId.Float -> pos += 4;
            case TypeId.Long, TypeId.Double -> pos += 8;
            case TypeId.String, TypeId.Generic -> {
                final int size = internalReadInt();
                pos += size;
            }
            default -> throw cannotSkip(typeId);
        }
    }

    private boolean internalReadBool() {
        return data[pos++] != 0;
    }

    private byte internalReadByte() {
        return data[pos++];
    }

    private short internalReadShort() {
        final short value = ShortType.readLE(data, pos);
        pos += 2;
        return value;
    }

    private char internalReadChar() {
        final char value = CharType.readLE(data, pos);
        pos += 2;
        return value;
    }

    private int internalReadInt() {
        final int value = IntType.readLE(data, pos);
        pos += 4;
        return value;
    }

    private long internalReadLong() {
        final long value = LongType.readLE(data, pos);
        pos += 8;
        return value;
    }

    private float internalReadFloat() {
        final float value = FloatType.readLE(data, pos);
        pos += 4;
        return value;
    }

    private double internalReadDouble() {
        final double value = DoubleType.readLE(data, pos);
        pos += 8;
        return value;
    }

    private String internalReadString() {
        final int size = internalReadInt();
        if (size == -1) {
            return null;
        }
        final String value = new String(data, pos, size, StandardCharsets.UTF_8);
        pos += size;
        return value;
    }

    private Object internalReadGeneric() {
        final int size = internalReadInt();
        final Object value = ObjectDecoderRegistry.decode(ByteString.copyFrom(data, pos, size));
        pos += size;
        return value;
    }

    private IllegalStateException cannotCast(
            final byte toType, final byte fromType, final int position) {
        return new IllegalStateException(
                "Cannot cast from "
                        + TypeId.toTypeName(fromType)
                        + " to "
                        + TypeId.toTypeName(toType)
                        + " at position "
                        + position);
    }

    private IllegalStateException cannotSkip(final byte type) {
        return new IllegalStateException("Cannot skip type " + TypeId.toTypeName(type));
    }
}
