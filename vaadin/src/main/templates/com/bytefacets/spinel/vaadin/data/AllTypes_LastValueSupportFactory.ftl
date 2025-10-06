<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.vaadin.data;
<#list types as type>
import com.bytefacets.collections.hash.Int${type.name}IndexedMap;
import com.bytefacets.collections.types.${type.name}Type;
import com.bytefacets.spinel.schema.${type.name}Field;
</#list>
import com.bytefacets.spinel.schema.SchemaField;
import com.bytefacets.spinel.schema.TypeId;

public final class LastValueSupportFactory {
    private LastValueSupportFactory() {}

    public static LastValueSupport lastValueSupport(final SchemaField schemaField,
                                                    final LastValueSupport.Callback callback) {
        return switch(schemaField.typeId()) {
            <#list types as type>
            case TypeId.${type.name} -> create${type.name}Support(schemaField, callback);
            </#list>
            default -> createGenericSupport(schemaField, callback);
        };
    }

<#list types as type>
    private static LastValueSupport create${type.name}Support(
                            final SchemaField schemaField, final LastValueSupport.Callback callback) {
        final Int${type.name}IndexedMap${type.instanceGenerics} lastValues = new Int${type.name}IndexedMap${type.instanceGenerics}(16);
        final ${type.name}Field field = (${type.name}Field) schemaField.field();
        return item -> {
            final int entry = lastValues.add(item.getRow());
            final ${type.arrayType} oldV = lastValues.getValueAt(entry);
            final ${type.arrayType} newV = field.valueAt(item.getRow());
            lastValues.putValueAt(entry, newV);
            return callback.callback(newV, oldV, ${type.name}Type.Asc.compare(oldV, newV));
        };
    }

</#list>

}