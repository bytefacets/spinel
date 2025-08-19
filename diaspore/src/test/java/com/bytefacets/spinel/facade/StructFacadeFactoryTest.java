// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.facade;

import static com.bytefacets.spinel.schema.ArrayFieldFactory.writableArrayField;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.bytefacets.spinel.gen.CodeGenException;
import com.bytefacets.spinel.schema.BoolField;
import com.bytefacets.spinel.schema.ByteField;
import com.bytefacets.spinel.schema.CharField;
import com.bytefacets.spinel.schema.DoubleField;
import com.bytefacets.spinel.schema.Field;
import com.bytefacets.spinel.schema.FieldChangeListener;
import com.bytefacets.spinel.schema.FieldList;
import com.bytefacets.spinel.schema.FloatField;
import com.bytefacets.spinel.schema.IntField;
import com.bytefacets.spinel.schema.LongField;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.SchemaBindable;
import com.bytefacets.spinel.schema.ShortField;
import com.bytefacets.spinel.schema.TypeId;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StructFacadeFactoryTest {
    private final StructFacadeFactory factory = StructFacadeFactory.structFacadeFactory();
    private Schema schema;

    @BeforeEach
    void setUp() {
        final Map<String, Field> fields = new HashMap<>();
        final FieldChangeListener noop = i -> {};
        for (int type = TypeId.Min, fieldId = 0; type <= TypeId.Max; type++, fieldId++) {
            final byte id = (byte) type;
            fields.put("Some" + TypeId.toTypeName(id), writableArrayField(id, 2, fieldId, noop));
        }
        schema = Schema.schema("foo", FieldList.fieldList(fields));
    }

    @Test
    void shouldThrowWhenReturnTypeDoesNotMatch() {
        final var ex =
                assertThrows(
                        CodeGenException.class,
                        () -> factory.createFacade(InvalidTypeMismatch.class));
        assertThat(
                ex.getMessage(),
                containsString(
                        "SomeString type inconsistent between get type(int) and set type(class java.lang.String)"));
    }

    @Test
    void shouldHandleSetterOnly() {
        final SetterOnly fac = factory.createFacade(SetterOnly.class);
        ((SchemaBindable) fac).bindToSchema(schema.asFieldResolver());
        ((StructFacade) fac).moveToRow(3);
        fac.setSomeString("Value");
        assertThat(schema.field("SomeString").objectValueAt(3), equalTo("Value"));
    }

    @Test
    void shouldHandleFluidSetter() {
        final FluidSetter fac = factory.createFacade(FluidSetter.class);
        ((SchemaBindable) fac).bindToSchema(schema.asFieldResolver());
        ((StructFacade) fac).moveToRow(3);
        fac.setSomeBool(true)
                .setSomeByte((byte) 5)
                .setSomeShort((short) 35)
                .setSomeChar('S')
                .setSomeInt(373)
                .setSomeLong(98393L)
                .setSomeFloat(45.3f)
                .setSomeDouble(777d)
                .setSomeString("Hello")
                .setSomeGeneric(new BigDecimal("32.2"));
        assertThat(((BoolField) schema.field("SomeBool").field()).valueAt(3), equalTo(true));
        assertThat(((ByteField) schema.field("SomeByte").field()).valueAt(3), equalTo((byte) 5));
        assertThat(
                ((ShortField) schema.field("SomeShort").field()).valueAt(3), equalTo((short) 35));
        assertThat(((CharField) schema.field("SomeChar").field()).valueAt(3), equalTo('S'));
        assertThat(((IntField) schema.field("SomeInt").field()).valueAt(3), equalTo(373));
        assertThat(((LongField) schema.field("SomeLong").field()).valueAt(3), equalTo(98393L));
        assertThat(((FloatField) schema.field("SomeFloat").field()).valueAt(3), equalTo(45.3f));
        assertThat(((DoubleField) schema.field("SomeDouble").field()).valueAt(3), equalTo(777d));
        assertThat(schema.field("SomeString").objectValueAt(3), equalTo("Hello"));
        assertThat(schema.field("SomeGeneric").objectValueAt(3), equalTo(new BigDecimal("32.2")));
    }

    @Test
    void shouldThrowWhenOtherThanGettersAndSetters() {
        final var ex =
                assertThrows(
                        CodeGenException.class, () -> factory.createFacade(InvalidNonGetter.class));
        assertThat(
                ex.getMessage(),
                containsString(
                        "methods found on interface that are not getters or setters -> 'random'"));
    }

    @Test
    void shouldThrowWhenNoUserFields() {
        final var ex =
                assertThrows(
                        CodeGenException.class,
                        () -> factory.createFacade(InvalidNoUserFields.class));
        assertThat(ex.getMessage(), containsString("no getters or setters found"));
    }

    public interface SetterOnly {
        void setSomeString(String value);
    }

    public interface FluidSetter {
        FluidSetter setSomeBool(boolean value);

        FluidSetter setSomeByte(byte value);

        FluidSetter setSomeShort(short value);

        FluidSetter setSomeChar(char value);

        FluidSetter setSomeInt(int value);

        FluidSetter setSomeLong(long value);

        FluidSetter setSomeFloat(float value);

        FluidSetter setSomeDouble(double value);

        FluidSetter setSomeString(String value);

        FluidSetter setSomeGeneric(Object value);
    }

    public interface InvalidNoUserFields {
        void setSomeString(); // not a valid setter: no arg

        void random(); // not a valid getter: no return type or get/is
    }

    public interface InvalidNonGetter {
        void setSomeString(String value);

        void random();
    }

    public interface InvalidTypeMismatch {
        void setSomeString(String value);

        int getSomeString();
    }
}
