// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.codec;

import static com.bytefacets.spinel.grpc.codec.InternalObjectEncoder.userEncoder;

import com.bytefacets.spinel.schema.TypeId;
import jakarta.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public final class ObjectEncoderRegistry {
    private static final Map<Class<?>, InternalObjectEncoder> DEFAULT_REGISTRY = new HashMap<>();

    static {
        DEFAULT_REGISTRY.put(Boolean.class, ObjectEncoderRegistry::encodeBool);
        DEFAULT_REGISTRY.put(Byte.class, ObjectEncoderRegistry::encodeByte);
        DEFAULT_REGISTRY.put(Short.class, ObjectEncoderRegistry::encodeShort);
        DEFAULT_REGISTRY.put(Character.class, ObjectEncoderRegistry::encodeChar);
        DEFAULT_REGISTRY.put(Integer.class, ObjectEncoderRegistry::encodeInteger);
        DEFAULT_REGISTRY.put(Long.class, ObjectEncoderRegistry::encodeLong);
        DEFAULT_REGISTRY.put(Float.class, ObjectEncoderRegistry::encodeFloat);
        DEFAULT_REGISTRY.put(Double.class, ObjectEncoderRegistry::encodeDouble);
        DEFAULT_REGISTRY.put(String.class, ObjectEncoderRegistry::encodeString);
    }

    private ObjectEncoderRegistry() {}

    public static void register(final Class<?> type, final ObjectEncoder encoder) {
        DEFAULT_REGISTRY.put(type, userEncoder(encoder));
    }

    @Nullable
    static InternalObjectEncoder lookup(@Nullable final Object value) {
        if (value == null) {
            return ObjectEncoderRegistry::encodeNull;
        } else {
            return DEFAULT_REGISTRY.get(value.getClass());
        }
    }

    static void encodeNull(final InternalBufferSupplier buffer, final Object ignored) {
        buffer.beginSystemType((byte) 0, (short) 0);
    }

    private static void encodeBool(final InternalBufferSupplier buffer, final Object value) {
        buffer.beginSystemType(TypeId.Bool, 1).put((byte) (((Boolean) value) ? 1 : 0));
    }

    private static void encodeByte(final InternalBufferSupplier buffer, final Object value) {
        buffer.beginSystemType(TypeId.Byte, 1).put((Byte) value);
    }

    private static void encodeShort(final InternalBufferSupplier buffer, final Object value) {
        buffer.beginSystemType(TypeId.Short, 2).putShort((Short) value);
    }

    private static void encodeChar(final InternalBufferSupplier buffer, final Object value) {
        buffer.beginSystemType(TypeId.Char, 2).putChar((Character) value);
    }

    private static void encodeInteger(final InternalBufferSupplier buffer, final Object value) {
        buffer.beginSystemType(TypeId.Int, 4).putInt((Integer) value);
    }

    private static void encodeLong(final InternalBufferSupplier buffer, final Object value) {
        buffer.beginSystemType(TypeId.Long, 8).putLong((Long) value);
    }

    private static void encodeFloat(final InternalBufferSupplier buffer, final Object value) {
        buffer.beginSystemType(TypeId.Float, 4).putFloat((Float) value);
    }

    private static void encodeDouble(final InternalBufferSupplier buffer, final Object value) {
        buffer.beginSystemType(TypeId.Double, 8).putDouble((Double) value);
    }

    private static void encodeString(final InternalBufferSupplier buffer, final Object value) {
        final var string = value.toString();
        final var bytes = string.getBytes(StandardCharsets.UTF_8);
        buffer.beginSystemType(TypeId.String, bytes.length).put(bytes);
    }
}
