// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.schema;

public final class TypeId {
    private TypeId() {}

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
            default -> "Object";
        };
    }
}
