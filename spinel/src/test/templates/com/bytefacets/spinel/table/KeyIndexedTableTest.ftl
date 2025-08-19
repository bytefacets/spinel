<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.table;

import static com.bytefacets.spinel.schema.FieldDescriptor.intField;
import static com.bytefacets.spinel.table.${type.name}IndexedTableBuilder.${type.name?lower_case}IndexedTable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.bytefacets.collections.types.${type.name}Type;
import com.bytefacets.spinel.exception.KeyException;
import com.bytefacets.spinel.exception.DuplicateKeyException;
import com.bytefacets.spinel.printer.OutputPrinter;
import com.bytefacets.spinel.schema.TypeId;
import com.bytefacets.spinel.validation.Key;
import com.bytefacets.spinel.validation.RowData;
import com.bytefacets.spinel.validation.ValidationOperator;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ${type.name}IndexedTableTest {
    private static final boolean print = false;
    private final ValidationOperator validation =
            new ValidationOperator(new String[] {"Id"}, "Value1", "Value2");
    private ${type.name}IndexedTable${instanceGenerics} table;
    private int value1FieldId;
    private int value2FieldId;
    private TableRow rowFacade;

    @BeforeEach
    void setUp() {
        table = ${type.name?lower_case}IndexedTable("table")
                    .addFields(intField("Value1"), intField("Value2"))
                    .keyFieldName("Id")
                    .build();
        value1FieldId = table.fieldId("Value1");
        value2FieldId = table.fieldId("Value2");
        rowFacade = table.tableRow();
        wire();
    }

    @AfterEach
    void tearDown() {
        validation.assertNoActiveValidation();
    }

    private void wire() {
        if(print) {
            table.output().attachInput(OutputPrinter.printer().input());
        }
        table.output().attachInput(validation.input());
        validation.expect().schema(expectedSchema()).validate();
        validation.clearChanges();
    }

    @Test
    void shouldLookupRowForKey() {
        addSourceRow(2, 20, 200);
        assertThat(table.lookupKeyRow(v(2)), equalTo(0));
        assertThat(table.lookupKeyRow(v(7)), equalTo(-1));
    }

    @Nested
    class AddTests {
        @Test
        void shouldAddRowForKey() {
            addSourceRow(1, 10, 100);
            table.fireChanges();
            validation.expect().added(key(1), rowData(10, 100)).validate();
        }

        @Test
        void shouldUpdateRowFacadePositionForAdd() {
            final int row = table.beginAdd(v(1));
            assertThat(row, equalTo(rowFacade.row()));
        }

        @Test
        void shouldThrowWhenAddingDuplicateKey() {
            addSourceRow(1, 10, 100);
            assertThrows(DuplicateKeyException.class, () -> table.beginAdd(v(1)));
        }
    }

    @Nested
    class ChangeTests {
        @BeforeEach
        void setUp() {
            addSourceRow(1, 10, 100);
            table.fireChanges();
            validation.clearChanges();
        }

        @Test
        void shouldChangeRowForKey() {
            changeValue1(1, 55);
            table.fireChanges();
            validation.expect().changed(key(1), new RowData(Map.of("Value1", 55))).validate();
        }

        @Test
        void shouldUpdateRowFacadePositionForChange() {
            final int row = table.beginChange(v(1));
            assertThat(row, equalTo(rowFacade.row()));
        }

        @Test
        void shouldThrowWhenChangingUnknownKey() {
            assertThrows(KeyException.class, () -> table.beginChange(v(5)));
        }

        @Test
        void shouldIncludeChangedFieldsOnlyOnChangedRow() {
            addSourceRow(2, 20, 200);
            changeValue1(1, 55);
            table.fireChanges();
            validation.expect().
                added(key(2), rowData(20, 200)).
                changed(key(1), new RowData(Map.of("Value1", 55))).validate();
        }
    }

    @Nested
    class RemoveTests {
        @Test
        void shouldRemoveRowForKey() {
            addSourceRow(1, 10, 100);
            table.fireChanges();
            validation.clearChanges();

            table.remove(v(1));
            table.fireChanges();
            validation.expect().removed(key(1)).validate();
        }

        @Test
        void shouldThrowWhenRemovingUnknownKey() {
            assertThrows(KeyException.class, () -> table.remove(v(5)));
        }
    }

    private Key key(final int key) {
        return new Key(List.of(v(key)));
    }

    private RowData rowData(final int value, final int value2) {
        return new RowData(Map.of("Value1", value, "Value2", value2));
    }

    private Map<String, Class<?>> expectedSchema() {
        final var keyType = TypeId.toClass(TypeId.${type.name});
        return Map.of("Id",
                      keyType,
                      "Value1",
                      Integer.class,
                      "Value2",
                      Integer.class);
    }

    private void addSourceRow(final int id, final int value1, final int value2) {
        final var row = table.tableRow();
        table.beginAdd(v(id));
        row.setInt(value1FieldId, value1);
        row.setInt(value2FieldId, value2);
        table.endAdd();
    }

    private void changeValue1(final int id, final int value1) {
        final var row = table.tableRow();
        table.beginChange(v(id));
        row.setInt(value1FieldId, value1);
        table.endChange();
    }

    private ${type.arrayType} v(final int x) {
        return ${type.name}Type.castTo${type.name}(x);
    }
}