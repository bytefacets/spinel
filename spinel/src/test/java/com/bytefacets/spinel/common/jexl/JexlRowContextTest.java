// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.common.jexl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.bytefacets.spinel.exception.FieldNotFoundException;
import com.bytefacets.spinel.schema.ArrayFieldFactory;
import com.bytefacets.spinel.schema.Field;
import com.bytefacets.spinel.schema.TypeId;
import java.util.Map;
import org.apache.commons.jexl3.JexlScript;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class JexlRowContextTest {
    @Test
    void shouldThrowWhenGettingUnmappedField() {
        final JexlRowContext context = context("b1 + b2 >= 6");
        final var ex = assertThrows(FieldNotFoundException.class, () -> context.get("foo"));
        assertThat(ex.getMessage(), containsString("foo"));
        assertThat(ex.getMessage(), containsString("b1 + b2 >= 6"));
    }

    @Test
    void shouldBindAndUnbind() {
        final JexlRowContext context = context("b1 + b2 >= 6");
        final Field b1 = field(TypeId.Int, 1, new int[] {1, 2});
        final Field b2 = field(TypeId.Int, 2, new int[] {3, 5});
        final Map<String, Field> map = Map.of("b1", b1, "b2", b2);
        context.bindToSchema(map::get);
        assertThat(context.boundFields(), containsInAnyOrder("b1", "b2"));
        context.setCurrentRow(1);
        assertThat(context.get("b1"), equalTo(2));
        assertThat(context.get("b2"), equalTo(5));
        context.unbindSchema();
        assertThat(context.boundFields(), empty());
    }

    @Nested
    class FieldTypeTests {
        @Test
        void shouldHandleBooleanField() {
            final JexlRowContext context = context("b1 && b2 != b3");
            final Field b1 = field(TypeId.Bool, 1, new boolean[] {true, false, true});
            final Field b2 = field(TypeId.Bool, 2, new boolean[] {false, true, false});
            final Field b3 = field(TypeId.Bool, 3, new boolean[] {true, false, false});
            final Map<String, Field> map = Map.of("b1", b1, "b2", b2, "b3", b3);
            context.bindToSchema(map::get);
            assertThat(context.evaluate(0), equalTo(true));
            assertThat(context.evaluate(1), equalTo(false));
            assertThat(context.evaluate(2), equalTo(false));
        }

        @Test
        void shouldHandleByteField() {
            final JexlRowContext context = context("b1 + b2 >= 6");
            final Field b1 = field(TypeId.Byte, 1, new byte[] {1, 2});
            final Field b2 = field(TypeId.Byte, 2, new byte[] {3, 5});
            final Map<String, Field> map = Map.of("b1", b1, "b2", b2);
            context.bindToSchema(map::get);
            assertThat(context.evaluate(0), equalTo(false));
            assertThat(context.evaluate(1), equalTo(true));
        }

        @Test
        void shouldHandleShortField() {
            final JexlRowContext context = context("b1 + b2 >= 6");
            final Field b1 = field(TypeId.Short, 1, new short[] {1, 2});
            final Field b2 = field(TypeId.Short, 2, new short[] {3, 5});
            final Map<String, Field> map = Map.of("b1", b1, "b2", b2);
            context.bindToSchema(map::get);
            assertThat(context.evaluate(0), equalTo(false));
            assertThat(context.evaluate(1), equalTo(true));
        }

        @Test
        void shouldHandleCharField() {
            // test uses String bc of jexl limitation
            final JexlRowContext context = context("b1 == \"c\"");
            final Field b1 = field(TypeId.Char, 1, new char[] {'a', 'c'});
            final Map<String, Field> map = Map.of("b1", b1);
            context.bindToSchema(map::get);
            assertThat(context.evaluate(0), equalTo(false));
            assertThat(context.evaluate(1), equalTo(true));
        }

        @Test
        void shouldHandleIntField() {
            final JexlRowContext context = context("b1 + b2 >= 6");
            final Field b1 = field(TypeId.Int, 1, new int[] {1, 2});
            final Field b2 = field(TypeId.Int, 2, new int[] {3, 5});
            final Map<String, Field> map = Map.of("b1", b1, "b2", b2);
            context.bindToSchema(map::get);
            assertThat(context.evaluate(0), equalTo(false));
            assertThat(context.evaluate(1), equalTo(true));
        }

        @Test
        void shouldHandleLongField() {
            final JexlRowContext context = context("b1 + b2 >= 6");
            final Field b1 = field(TypeId.Long, 1, new long[] {1, 2});
            final Field b2 = field(TypeId.Long, 2, new long[] {3, 5});
            final Map<String, Field> map = Map.of("b1", b1, "b2", b2);
            context.bindToSchema(map::get);
            assertThat(context.evaluate(0), equalTo(false));
            assertThat(context.evaluate(1), equalTo(true));
        }

        @Test
        void shouldHandleFloatField() {
            final JexlRowContext context = context("b1 + b2 >= 6.3");
            final Field b1 = field(TypeId.Float, 1, new float[] {1, 2.2f});
            final Field b2 = field(TypeId.Float, 2, new float[] {3, 5.2f});
            final Map<String, Field> map = Map.of("b1", b1, "b2", b2);
            context.bindToSchema(map::get);
            assertThat(context.evaluate(0), equalTo(false));
            assertThat(context.evaluate(1), equalTo(true));
        }

        @Test
        void shouldHandleDoubleField() {
            final JexlRowContext context = context("b1 + b2 >= 6.3");
            final Field b1 = field(TypeId.Double, 1, new double[] {1, 2.2f});
            final Field b2 = field(TypeId.Double, 2, new double[] {3, 5.2f});
            final Map<String, Field> map = Map.of("b1", b1, "b2", b2);
            context.bindToSchema(map::get);
            assertThat(context.evaluate(0), equalTo(false));
            assertThat(context.evaluate(1), equalTo(true));
        }

        @Test
        void shouldHandleStringField() {
            final JexlRowContext context = context("b1 >= \"foo\"");
            final Field b1 = field(TypeId.String, 1, new String[] {"bar", "foo", "zig"});
            final Map<String, Field> map = Map.of("b1", b1);
            context.bindToSchema(map::get);
            assertThat(context.evaluate(0), equalTo(false));
            assertThat(context.evaluate(1), equalTo(true));
            assertThat(context.evaluate(2), equalTo(true));
        }

        @Test
        void shouldHandleGenericField() {
            final JexlRowContext context = context("b1 + b2 >= 6.3");
            final Field b1 = field(TypeId.Generic, 1, new Object[] {(byte) 7, (short) 8, 9L});
            final Field b2 = field(TypeId.Generic, 2, new Object[] {-1, -1.5, 6f});
            final Map<String, Field> map = Map.of("b1", b1, "b2", b2);
            context.bindToSchema(map::get);
            assertThat(context.evaluate(0), equalTo(false));
            assertThat(context.evaluate(1), equalTo(true));
            assertThat(context.evaluate(2), equalTo(true));
        }
    }

    private JexlRowContext context(final String expression) {
        return new JexlRowContext(script(expression));
    }

    private JexlScript script(final String expression) {
        return JexlEngineProvider.defaultJexlEngine().createScript(expression);
    }

    private Field field(final byte typeId, final int fieldId, final Object values) {
        return ArrayFieldFactory.writableArrayFieldOver(typeId, values, fieldId, i -> {});
    }
}
