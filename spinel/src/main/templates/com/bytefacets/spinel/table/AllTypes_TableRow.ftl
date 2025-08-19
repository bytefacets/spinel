<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.table;

import com.bytefacets.spinel.schema.*;
import java.util.Objects;

public final class TableRow {
    static final int NO_ROW = -1;
    private final FieldList fieldList;
    int currentRow = -1;

    TableRow(final FieldList fieldList) {
        this.fieldList = Objects.requireNonNull(fieldList, "fieldList");
    }

    public int row() {
        return currentRow;
    }

    public void setRow(final int row) {
        this.currentRow = row;
    }

    void setNoRow() {
        this.currentRow = NO_ROW;
    }

<#list types as type>
    public void set${type.name}(final int fieldId, final ${type.arrayType} value) {
        final SchemaField sField = fieldList.fieldAt(fieldId);
        final var field = (${type.name}WritableField)sField.field();
        field.setValueAt(currentRow, value);
    }

</#list>

}