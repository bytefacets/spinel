<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.schema;

<#list types as type>
import com.bytefacets.collections.types.${type.name}Type;
import com.bytefacets.collections.arrays.${type.name}Array;
</#list>

import java.util.Objects;

public final class ArrayFieldFactory {
    private ArrayFieldFactory() {
    }
    public static Field writableArrayField(final byte typeId,
                                           final int initialSize,
                                           final int fieldId,
                                           final FieldChangeListener changeListener) {
        return switch(typeId) {
<#list types as type>
            case TypeId.${type.name} ->
                writable${type.name}ArrayField(initialSize, fieldId, changeListener);
</#list>
            default -> throw new IllegalArgumentException("Unknown typeId: " + typeId);
        };
    }

    public static Field writableArrayFieldOver(final byte typeId,
                                           final Object initialValues,
                                           final int fieldId,
                                           final FieldChangeListener changeListener) {
        return switch(typeId) {
<#list types as type>
            case TypeId.${type.name} ->
                writable${type.name}ArrayField((${type.arrayType}[]) initialValues, fieldId,
                                               changeListener, ${type.name}Type.DEFAULT);
</#list>
            default -> throw new IllegalArgumentException("Unknown typeId: " + typeId);
        };
    }

<#list types as type>
    public static ${type.name}WritableField writable${type.name}ArrayField(
            final int initialSize,
            final int fieldId,
            final FieldChangeListener changeListener) {
        return writable${type.name}ArrayField(initialSize, fieldId, changeListener, ${type.name}Type.DEFAULT);
    }

    public static ${type.name}WritableField writable${type.name}ArrayField(
            final int initialSize,
            final int fieldId,
            final FieldChangeListener changeListener,
            final ${type.arrayType} initialValue) {
        return new ${type.name}ArrayField(${type.name}Array.create(initialSize, initialValue),
                                          fieldId, changeListener, initialValue);
    }

    public static ${type.name}WritableField writable${type.name}ArrayField(
            final ${type.arrayType}[] initialValues,
            final int fieldId,
            final FieldChangeListener changeListener,
            final ${type.arrayType} initialValue) {
        return new ${type.name}ArrayField(initialValues, fieldId, changeListener, initialValue);
    }

</#list>
<#list types as type>
    public static class ${type.name}ArrayField implements ${type.name}WritableField {
        private final ${type.arrayType} initialValue;
        private ${type.arrayType}[] values;
        private final FieldChangeListener listener;
        private final int fieldId;
        private ${type.name}ArrayField(
                    ${type.arrayType}[] initialValues,
                    final int fieldId,
                    final FieldChangeListener listener,
                    ${type.arrayType} initialValue) {
            this.values = Objects.requireNonNull(initialValues, "initialValues");
            this.fieldId = fieldId;
            this.listener = Objects.requireNonNull(listener, "listener");
            this.initialValue = initialValue;
        }

        @Override
        public ${type.arrayType} valueAt(final int row) {
            if(row >= 0 && row < values.length) {
                return values[row];
            } else {
                return initialValue;
            }
        }

        @Override
        public void setValueAt(final int row, final ${type.arrayType} value) {
            values = ${type.name}Array.ensureEntry(values, row, initialValue);
            listener.fieldChanged(fieldId);
            values[row] = value;
        }
    }

</#list>
}
