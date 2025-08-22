// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.schema;

import java.util.Objects;
import jakarta.annotation.Nullable;

public interface FieldResolver {
    @Nullable
    Field findField(String name);

    default Field getField(String name) {
        return Objects.requireNonNull(findField(name), name + " not found in schema");
    }

    default BoolField findBoolField(final String name) {
        final var field = findField(name);
        return field != null ? Cast.toBoolField(field) : null;
    }

    default ByteField findByteField(final String name) {
        final var field = findField(name);
        return field != null ? Cast.toByteField(field) : null;
    }

    default ShortField findShortField(final String name) {
        final var field = findField(name);
        return field != null ? Cast.toShortField(field) : null;
    }

    default CharField findCharField(final String name) {
        final var field = findField(name);
        return field != null ? Cast.toCharField(field) : null;
    }

    default IntField findIntField(final String name) {
        final var field = findField(name);
        return field != null ? Cast.toIntField(field) : null;
    }

    default LongField findLongField(final String name) {
        final var field = findField(name);
        return field != null ? Cast.toLongField(field) : null;
    }

    default FloatField findFloatField(final String name) {
        final var field = findField(name);
        return field != null ? Cast.toFloatField(field) : null;
    }

    default DoubleField findDoubleField(final String name) {
        final var field = findField(name);
        return field != null ? Cast.toDoubleField(field) : null;
    }

    default StringField findStringField(final String name) {
        final var field = findField(name);
        return field != null ? Cast.toStringField(field) : null;
    }

    default GenericField findGenericField(final String name) {
        final var field = findField(name);
        return field != null ? Cast.toGenericField(field) : null;
    }
}
