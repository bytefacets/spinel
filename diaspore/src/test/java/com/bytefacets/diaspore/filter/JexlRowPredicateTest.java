// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.filter;

import static com.bytefacets.diaspore.filter.JexlRowPredicate.jexlPredicate;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.bytefacets.diaspore.common.jexl.JexlRowContext;
import com.bytefacets.diaspore.exception.FieldNotFoundException;
import com.bytefacets.diaspore.schema.ArrayFieldFactory;
import com.bytefacets.diaspore.schema.Field;
import com.bytefacets.diaspore.schema.TypeId;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class JexlRowPredicateTest {

    @Nested
    class BindingTests {
        @Test
        void shouldCallbackWhenFieldNotFound() {
            final JexlRowPredicate predicate = jexlPredicate("b1 && b2 != b3");
            final Field b1 = field(TypeId.Bool, 1, new boolean[] {true, false, true});
            final Field b3 = field(TypeId.Bool, 3, new boolean[] {true, false, false});
            final Map<String, Field> map = Map.of("b1", b1, "b3", b3);
            predicate.bindToSchema(map::get);
            final var ex = assertThrows(FieldNotFoundException.class, () -> predicate.testRow(0));
            assertThat(ex.getMessage(), containsString("b2"));
        }

        @Test
        void shouldUnbind() {
            final JexlRowContext context = mock(JexlRowContext.class);
            final JexlRowPredicate predicate =
                    new JexlRowPredicate(
                            JexlRowPredicate.DEFAULT_ENGINE.createScript("b1 && b2"), context);
            final Field b1 = field(TypeId.Bool, 1, new boolean[] {true, false, true});
            final Field b2 = field(TypeId.Bool, 2, new boolean[] {false, true, false});
            final Map<String, Field> map = Map.of("b1", b1, "b2", b2);
            predicate.bindToSchema(map::get);
            verify(context, times(1)).bindToSchema(any());
            predicate.unbindSchema();
            verify(context, times(1)).unbindSchema();
        }
    }

    @Nested
    class FieldTypeTests {
        @Test
        void shouldHandleBooleanField() {
            final JexlRowPredicate predicate = jexlPredicate("b1 && b2 != b3");
            final Field b1 = field(TypeId.Bool, 1, new boolean[] {true, false, true});
            final Field b2 = field(TypeId.Bool, 2, new boolean[] {false, true, false});
            final Field b3 = field(TypeId.Bool, 3, new boolean[] {true, false, false});
            final Map<String, Field> map = Map.of("b1", b1, "b2", b2, "b3", b3);
            predicate.bindToSchema(map::get);
            assertThat(predicate.testRow(0), equalTo(true));
            assertThat(predicate.testRow(1), equalTo(false));
            assertThat(predicate.testRow(2), equalTo(false));
        }

        @Test
        void shouldHandleByteField() {
            final JexlRowPredicate predicate = jexlPredicate("b1 + b2 >= 6");
            final Field b1 = field(TypeId.Byte, 1, new byte[] {1, 2});
            final Field b2 = field(TypeId.Byte, 2, new byte[] {3, 5});
            final Map<String, Field> map = Map.of("b1", b1, "b2", b2);
            predicate.bindToSchema(map::get);
            assertThat(predicate.testRow(0), equalTo(false));
            assertThat(predicate.testRow(1), equalTo(true));
        }

        @Test
        void shouldHandleShortField() {
            final JexlRowPredicate predicate = jexlPredicate("b1 + b2 >= 6");
            final Field b1 = field(TypeId.Short, 1, new short[] {1, 2});
            final Field b2 = field(TypeId.Short, 2, new short[] {3, 5});
            final Map<String, Field> map = Map.of("b1", b1, "b2", b2);
            predicate.bindToSchema(map::get);
            assertThat(predicate.testRow(0), equalTo(false));
            assertThat(predicate.testRow(1), equalTo(true));
        }

        @Test
        void shouldHandleCharField() {
            final JexlRowPredicate predicate = jexlPredicate("b1 == 'c'");
            final Field b1 = field(TypeId.Char, 1, new char[] {'a', 'c'});
            final Map<String, Field> map = Map.of("b1", b1);
            predicate.bindToSchema(map::get);
            assertThat(predicate.testRow(0), equalTo(false));
            assertThat(predicate.testRow(1), equalTo(true));
        }

        @Test
        void shouldHandleIntField() {
            final JexlRowPredicate predicate = jexlPredicate("b1 + b2 >= 6");
            final Field b1 = field(TypeId.Int, 1, new int[] {1, 2});
            final Field b2 = field(TypeId.Int, 2, new int[] {3, 5});
            final Map<String, Field> map = Map.of("b1", b1, "b2", b2);
            predicate.bindToSchema(map::get);
            assertThat(predicate.testRow(0), equalTo(false));
            assertThat(predicate.testRow(1), equalTo(true));
        }

        @Test
        void shouldHandleLongField() {
            final JexlRowPredicate predicate = jexlPredicate("b1 + b2 >= 6");
            final Field b1 = field(TypeId.Long, 1, new long[] {1, 2});
            final Field b2 = field(TypeId.Long, 2, new long[] {3, 5});
            final Map<String, Field> map = Map.of("b1", b1, "b2", b2);
            predicate.bindToSchema(map::get);
            assertThat(predicate.testRow(0), equalTo(false));
            assertThat(predicate.testRow(1), equalTo(true));
        }

        @Test
        void shouldHandleFloatField() {
            final JexlRowPredicate predicate = jexlPredicate("b1 + b2 >= 6.3");
            final Field b1 = field(TypeId.Float, 1, new float[] {1, 2.2f});
            final Field b2 = field(TypeId.Float, 2, new float[] {3, 5.2f});
            final Map<String, Field> map = Map.of("b1", b1, "b2", b2);
            predicate.bindToSchema(map::get);
            assertThat(predicate.testRow(0), equalTo(false));
            assertThat(predicate.testRow(1), equalTo(true));
        }

        @Test
        void shouldHandleDoubleField() {
            final JexlRowPredicate predicate = jexlPredicate("b1 + b2 >= 6.3");
            final Field b1 = field(TypeId.Double, 1, new double[] {1, 2.2f});
            final Field b2 = field(TypeId.Double, 2, new double[] {3, 5.2f});
            final Map<String, Field> map = Map.of("b1", b1, "b2", b2);
            predicate.bindToSchema(map::get);
            assertThat(predicate.testRow(0), equalTo(false));
            assertThat(predicate.testRow(1), equalTo(true));
        }

        @Test
        void shouldHandleStringField() {
            final JexlRowPredicate predicate = jexlPredicate("b1 >= \"foo\"");
            final Field b1 = field(TypeId.String, 1, new String[] {"bar", "foo", "zig"});
            final Map<String, Field> map = Map.of("b1", b1);
            predicate.bindToSchema(map::get);
            assertThat(predicate.testRow(0), equalTo(false));
            assertThat(predicate.testRow(1), equalTo(true));
            assertThat(predicate.testRow(2), equalTo(true));
        }

        @Test
        void shouldHandleGenericField() {
            final JexlRowPredicate predicate = jexlPredicate("b1 + b2 >= 6.3");
            final Field b1 = field(TypeId.Generic, 1, new Object[] {(byte) 7, (short) 8, 9L});
            final Field b2 = field(TypeId.Generic, 2, new Object[] {-1, -1.5, 6f});
            final Map<String, Field> map = Map.of("b1", b1, "b2", b2);
            predicate.bindToSchema(map::get);
            assertThat(predicate.testRow(0), equalTo(false));
            assertThat(predicate.testRow(1), equalTo(true));
            assertThat(predicate.testRow(2), equalTo(true));
        }
    }

    Field field(final byte typeId, final int fieldId, final Object values) {
        return ArrayFieldFactory.writableArrayField(typeId, values, fieldId, i -> {});
    }
}
