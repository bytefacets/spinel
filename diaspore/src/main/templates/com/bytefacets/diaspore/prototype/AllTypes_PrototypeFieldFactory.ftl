<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.prototype;

import com.bytefacets.diaspore.schema.Field;
import com.bytefacets.diaspore.schema.FieldResolver;
import com.bytefacets.diaspore.schema.SchemaBindable;
<#list types as type>
import com.bytefacets.collections.types.${type.name}Type;
import com.bytefacets.diaspore.schema.${type.name}Field;
</#list>
import com.bytefacets.diaspore.schema.TypeId;

import java.util.Objects;

import static com.bytefacets.diaspore.exception.FieldNotFoundException.fieldNotFound;

final class PrototypeFieldFactory {
    private PrototypeFieldFactory() {}

    interface SchemaProvider {
        void registerForSchema(SchemaBindable bindable);
    }

    // improve later in java21
    static Field createPrototypeField(final byte typeId, final String name, final SchemaProvider schemaProvider) {
        return switch(typeId) {
<#list types as type>
            case TypeId.${type.name} -> create${type.name}Prototype(name, schemaProvider);
</#list>
            default -> throw new IllegalArgumentException("Unknown typeId: " + typeId);
        };
    }

<#list types as type>
    private static final ${type.name}Field DEFAULT_FIELD_${type.name?upper_case} = row -> ${type.name}Type.DEFAULT;

    private static ${type.name}Field create${type.name}Prototype(final String name, final SchemaProvider schemaProvider) {
        final var field = new Prototype${type.name}Field(name);
        schemaProvider.registerForSchema(field.binding());
        return field;
    }

    private static final class Prototype${type.name}Field implements ${type.name}Field {
        private final String name;
        private ${type.name}Field currentField = DEFAULT_FIELD_${type.name?upper_case};

        private Prototype${type.name}Field(final String name) {
            this.name = Objects.requireNonNull(name, "name");
        }

        private SchemaBindable binding() {
            return new SchemaBindable() {
                @Override
                public void bindToSchema(final FieldResolver fieldResolver) {
                    currentField = fieldResolver.find${type.name}Field(name);
                    if(currentField == null) {
                        throw fieldNotFound(name);
                    }
                }

                @Override
                public void unbindSchema() {
                    currentField = DEFAULT_FIELD_${type.name?upper_case};
                }
            };
        }

        public ${type.arrayType} valueAt(final int row) {
            return currentField.valueAt(row);
        }
    }
</#list>
}

