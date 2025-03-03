<#ftl strip_whitespace=true>
package com.bytefacets.diaspore.testing;
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT

import com.bytefacets.diaspore.table.${type.name}IndexedTable;
import com.bytefacets.diaspore.table.TableRow;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public final class ${type.name}TableHandle {
    private final ${type.name}IndexedTable${instanceGenerics} table;
    private final List<ObjectFieldAdapter> adapters;

    private ${type.name}TableHandle(final String keyName, final ${type.name}IndexedTable${instanceGenerics} table) {
        this.table = requireNonNull(table, "table");
        this.adapters = new ArrayList<>(table.schema().size());
        table.schema().forEachField(field -> {
            if(!field.name().equals(keyName)) {
                adapters.add(ObjectFieldAdapterFactory.createAdapter(field));
            }
        });
    }

    public static ${type.name}TableHandle ${type.name?lower_case}TableHandle(final String keyName, final ${type.name}IndexedTable${instanceGenerics} table) {
        return new ${type.name}TableHandle(keyName, table);
    }

    public ${type.name}TableHandle add(final ${type.arrayType} key, final Object... values) {
        final TableRow row = table.tableRow();
        table.beginAdd(key);
        for(int i = 0, len = Math.min(values.length, adapters.size()); i < len; i++) {
            adapters.get(i).apply(row, values[i]);
        }
        table.endAdd();
        return this;
    }

    public ${type.name}TableHandle change(final ${type.arrayType} key, final Object... values) {
        final TableRow row = table.tableRow();
        table.beginChange(key);
        for(int i = 0, len = Math.min(values.length, adapters.size()); i < len; i++) {
            if(values[i] != null) {
                adapters.get(i).apply(row, values[i]);
            }
        }
        table.endChange();
        return this;
    }

    public ${type.name}TableHandle remove(final ${type.arrayType} key) {
        table.remove(key);
        return this;
    }

    public void fire() {
        this.table.fireChanges();
    }
}
