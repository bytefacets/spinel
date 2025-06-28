<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.table;

import static com.bytefacets.diaspore.table.${type.name}IndexedStructTableBuilder.${type.name?lower_case}IndexedStructTable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.bytefacets.collections.types.${type.name}Type;
import com.bytefacets.diaspore.exception.KeyException;
import com.bytefacets.diaspore.exception.DuplicateKeyException;
import com.bytefacets.diaspore.facade.StructFacade;
import com.bytefacets.diaspore.printer.OutputPrinter;
import com.bytefacets.diaspore.schema.TypeId;
import com.bytefacets.diaspore.validation.Key;
import com.bytefacets.diaspore.validation.RowData;
import com.bytefacets.diaspore.validation.ValidationOperator;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

<#if type.name == "Generic">
    <#assign instanceGenerics="<Object, MyValue>">
<#else>
    <#assign instanceGenerics="<MyValue>">
</#if>
class ${type.name}IndexedStructTableTest {
    private static final boolean print = false;
    private final ValidationOperator validation =
            new ValidationOperator(new String[] {"Id"}, "Value1", "Value2");
    private ${type.name}IndexedStructTable${instanceGenerics} table;
    private MyValue rowFacade;

    @BeforeEach
    void setUp() {
        table = ${type.name?lower_case}IndexedStructTable(MyValue.class).build();
        rowFacade = table.createFacade();
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
        void shouldUpsertRowForKey() {
            upsertSourceRow(1, 10, 100);
            table.fireChanges();
            validation.expect().added(key(1), rowData(10, 100)).validate();
        }

        @Test
        void shouldUpdateRowFacadePositionForAdd() {
            ((StructFacade)rowFacade).moveToRow(27);
            table.beginAdd(v(1), rowFacade);
            assertThat(0, equalTo(((StructFacade)rowFacade).currentRow()));
        }

        @Test
        void shouldThrowWhenAddingDuplicateKey() {
            addSourceRow(1, 10, 100);
            assertThrows(DuplicateKeyException.class, () -> table.beginAdd(v(1), rowFacade));
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
        void shouldUpsertRowForKey() {
            upsertSourceRow(1, 10, 100);
            table.fireChanges();
            validation.expect().changed(key(1), rowData(10, 100)).validate();
        }

        @Test
        void shouldUpdateRowFacadePositionForChange() {
            ((StructFacade)rowFacade).moveToRow(27);
            table.beginChange(v(1), rowFacade);
            assertThat(0, equalTo(((StructFacade)rowFacade).currentRow()));
        }

        @Test
        void shouldThrowWhenChangingUnknownKey() {
            assertThrows(KeyException.class, () -> table.beginChange(v(5), rowFacade));
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

    interface MyValue {
        ${type.arrayType} getId(); // the key field

        int getValue1();
        int getValue2();
        MyValue setValue1(int value);
        MyValue setValue2(int value);
    }

    private void addSourceRow(final int id, final int value1, final int value2) {
        table.beginAdd(v(id), rowFacade).setValue1(value1).setValue2(value2);
        table.endAdd();
    }

    private void changeValue1(final int id, final int value1) {
        table.beginChange(v(id), rowFacade).setValue1(value1);
        table.endChange();
    }

    private void upsertSourceRow(final int id, final int value1, final int value2) {
        table.beginUpsert(v(id), rowFacade).setValue1(value1).setValue2(value2);
        table.endUpsert();
    }

    private ${type.arrayType} v(final int x) {
        return ${type.name}Type.castTo${type.name}(x);
    }
}