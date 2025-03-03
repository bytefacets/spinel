<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.union;

import com.bytefacets.collections.arrays.GenericArray;
<#list types as type>
import com.bytefacets.diaspore.schema.${type.name}Field;
</#list>
import com.bytefacets.diaspore.schema.Field;
import com.bytefacets.diaspore.schema.Cast;
import com.bytefacets.diaspore.schema.TypeId;

import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

final class UnionFieldFactory {
    private UnionFieldFactory() {
    }

    static UnionField createUnionField(final int typeId, final UnionRowMapper mapper) {
        return switch(typeId) {
<#list types as type>
            case TypeId.${type.name} -> new ${type.name}UnionField(mapper);
</#list>
            default -> throw new IllegalArgumentException("Unknown typeId: " + typeId);
        };
    }

<#list types as type>
    static final class ${type.name}UnionField implements ${type.name}Field, UnionField {
        private final UnionRowMapper mapper;
        private ${type.name}Field[] inputFields = new ${type.name}Field[2];

        ${type.name}UnionField(final UnionRowMapper mapper) {
            this.mapper = requireNonNull(mapper, "mapper");
        }

        @Override
        public void setField(final int inputIndex, @Nullable final Field field) {
            inputFields = GenericArray.ensureEntry(inputFields, inputIndex);
            inputFields[inputIndex] = field != null ? Cast.to${type.name}Field(field) : null;
        }

        @Override
        public ${type.arrayType} valueAt(final int row) {
            final int inputIndex = mapper.inputIndexOf(row);
            final int inputRow = mapper.inputRowOf(row);
            return inputFields[inputIndex].valueAt(inputRow);
        }
    }

</#list>
}
