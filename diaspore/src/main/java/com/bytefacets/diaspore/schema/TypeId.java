// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.schema;

import java.util.HashMap;
import java.util.Map;

public final class TypeId {
    private static final Map<Class<?>, Byte> mapping = new HashMap<>(32);

    static {
        mapping.put(boolean.class, TypeId.Bool);
        mapping.put(Boolean.class, TypeId.Bool);

        mapping.put(byte.class, TypeId.Byte);
        mapping.put(Byte.class, TypeId.Byte);

        mapping.put(short.class, TypeId.Short);
        mapping.put(Short.class, TypeId.Short);

        mapping.put(char.class, TypeId.Char);
        mapping.put(Character.class, TypeId.Char);

        mapping.put(int.class, TypeId.Int);
        mapping.put(Integer.class, TypeId.Int);

        mapping.put(long.class, TypeId.Long);
        mapping.put(Long.class, TypeId.Long);

        mapping.put(float.class, TypeId.Float);
        mapping.put(Float.class, TypeId.Float);

        mapping.put(double.class, TypeId.Double);
        mapping.put(Double.class, TypeId.Double);

        mapping.put(String.class, TypeId.String);
        mapping.put(Object.class, TypeId.Generic);
    }

    private TypeId() {}

    public static final int Min = 1;
    public static final byte Bool = 1;
    public static final byte Byte = 2;
    public static final byte Short = 3;
    public static final byte Char = 4;
    public static final byte Int = 5;
    public static final byte Long = 6;
    public static final byte Float = 7;
    public static final byte Double = 8;
    public static final byte String = 9;
    public static final byte Generic = 10;
    public static final int Max = 10;

    public static byte toId(final Class<?> type) {
        return mapping.getOrDefault(type, TypeId.Generic);
    }

    public static Class<?> toClass(final byte id) {
        return switch (id) {
            case Bool -> Boolean.class;
            case Byte -> Byte.class;
            case Short -> Short.class;
            case Char -> Character.class;
            case Int -> Integer.class;
            case Long -> Long.class;
            case Float -> Float.class;
            case Double -> Double.class;
            case String -> String.class;
            default -> Object.class;
        };
    }

    public static String toTypeName(final byte id) {
        return switch (id) {
            case Bool -> "Bool";
            case Byte -> "Byte";
            case Short -> "Short";
            case Char -> "Char";
            case Int -> "Int";
            case Long -> "Long";
            case Float -> "Float";
            case Double -> "Double";
            case String -> "String";
            default -> "Generic";
        };
    }
}
