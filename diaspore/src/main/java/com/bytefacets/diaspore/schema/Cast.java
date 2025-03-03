// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.schema;

import java.util.Objects;

public final class Cast {
    private Cast() {}

    public static BoolField toBoolField(final Field field) {
        if (field instanceof BoolField castField) {
            return castField;
        } else if (field instanceof ByteField castField) {
            return row -> castField.valueAt(row) != 0;
        } else if (field instanceof ShortField castField) {
            return row -> castField.valueAt(row) != 0;
        } else if (field instanceof CharField castField) {
            return row -> castField.valueAt(row) != 'T' && castField.valueAt(row) != 't';
        } else if (field instanceof IntField castField) {
            return row -> castField.valueAt(row) != 0;
        } else {
            throw new CastException(field, "Bool");
        }
    }

    public static ByteField toByteField(final Field field) {
        if (field instanceof ByteField castField) {
            return castField;
        } else if (field instanceof BoolField castField) {
            return row -> (byte) (castField.valueAt(row) ? 1 : 0);
        } else if (field instanceof CharField castField) {
            return row -> (byte) castField.valueAt(row);
        } else {
            throw new CastException(field, "Byte");
        }
    }

    public static ShortField toShortField(final Field field) {
        if (field instanceof ShortField castField) {
            return castField;
        } else if (field instanceof ByteField castField) {
            return castField::valueAt;
        } else if (field instanceof BoolField castField) {
            return row -> (short) (castField.valueAt(row) ? 1 : 0);
        } else if (field instanceof CharField castField) {
            return row -> (short) castField.valueAt(row);
        } else {
            throw new CastException(field, "Short");
        }
    }

    public static CharField toCharField(final Field field) {
        if (field instanceof CharField castField) {
            return castField;
        } else if (field instanceof ByteField castField) {
            return row -> (char) castField.valueAt(row);
        } else if (field instanceof ShortField castField) {
            return row -> (char) castField.valueAt(row);
        } else if (field instanceof BoolField castField) {
            return row -> castField.valueAt(row) ? 'T' : 'F';
        } else if (field instanceof StringField castField) {
            return row -> {
                final String val = castField.valueAt(row);
                return val != null && !val.isEmpty() ? val.charAt(0) : '\0';
            };
        } else {
            throw new CastException(field, "Char");
        }
    }

    public static IntField toIntField(final Field field) {
        if (field instanceof IntField castField) {
            return castField;
        } else if (field instanceof ShortField castField) {
            return castField::valueAt;
        } else if (field instanceof ByteField castField) {
            return castField::valueAt;
        } else if (field instanceof CharField castField) {
            return row -> (int) castField.valueAt(row);
        } else if (field instanceof BoolField castField) {
            return row -> castField.valueAt(row) ? 1 : 0;
        } else {
            throw new CastException(field, "Int");
        }
    }

    public static LongField toLongField(final Field field) {
        if (field instanceof LongField castField) {
            return castField;
        } else if (field instanceof IntField castField) {
            return castField::valueAt;
        } else if (field instanceof ShortField castField) {
            return castField::valueAt;
        } else if (field instanceof ByteField castField) {
            return castField::valueAt;
        } else if (field instanceof CharField castField) {
            return row -> (long) castField.valueAt(row);
        } else if (field instanceof BoolField castField) {
            return row -> castField.valueAt(row) ? 1L : 0L;
        } else {
            throw new CastException(field, "Long");
        }
    }

    public static FloatField toFloatField(final Field field) {
        if (field instanceof FloatField castField) {
            return castField;
        } else if (field instanceof IntField castField) {
            return castField::valueAt;
        } else if (field instanceof ShortField castField) {
            return castField::valueAt;
        } else if (field instanceof ByteField castField) {
            return castField::valueAt;
        } else if (field instanceof BoolField castField) {
            return row -> (float) (castField.valueAt(row) ? 1 : 0);
        } else {
            throw new CastException(field, "Float");
        }
    }

    public static DoubleField toDoubleField(final Field field) {
        if (field instanceof DoubleField castField) {
            return castField;
        } else if (field instanceof FloatField castField) {
            return castField::valueAt;
        } else if (field instanceof IntField castField) {
            return castField::valueAt;
        } else if (field instanceof ShortField castField) {
            return castField::valueAt;
        } else if (field instanceof ByteField castField) {
            return castField::valueAt;
        } else if (field instanceof BoolField castField) {
            return row -> (float) (castField.valueAt(row) ? 1 : 0);
        } else {
            throw new CastException(field, "Double");
        }
    }

    public static StringField toStringField(final Field field) {
        if (field instanceof StringField castField) {
            return castField;
        } else if (field instanceof GenericField castField) {
            return row -> Objects.toString(castField.valueAt(row));
        } else if (field instanceof DoubleField castField) {
            return row -> Double.toString(castField.valueAt(row));
        } else if (field instanceof FloatField castField) {
            return row -> Float.toString(castField.valueAt(row));
        } else if (field instanceof IntField castField) {
            return row -> Integer.toString(castField.valueAt(row));
        } else if (field instanceof ShortField castField) {
            return row -> Short.toString(castField.valueAt(row));
        } else if (field instanceof ByteField castField) {
            return row -> Byte.toString(castField.valueAt(row));
        } else if (field instanceof BoolField castField) {
            return row -> Boolean.toString(castField.valueAt(row));
        } else if (field instanceof CharField castField) {
            return row -> Character.toString(castField.valueAt(row));
        } else {
            throw new CastException(field, "String");
        }
    }

    public static GenericField toGenericField(final Field field) {
        if (field instanceof GenericField castField) {
            return castField;
        } else {
            return field::objectValueAt;
        }
    }

    public static final class CastException extends RuntimeException {
        private CastException(final Field from, final String to) {
            super(String.format("Cannot cast from %s to %sField", from.getClass(), to));
        }
    }
}
