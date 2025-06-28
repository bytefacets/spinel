<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.interner;

import com.bytefacets.collections.types.*;
import com.bytefacets.diaspore.schema.ArrayFieldFactory;
import com.bytefacets.diaspore.schema.Field;
import com.bytefacets.diaspore.schema.FieldResolver;
import com.bytefacets.diaspore.schema.TypeId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class TupleInternerTest {
    private static final byte[] FIXED_1 = new byte[] { TypeId.Bool, TypeId.Short, TypeId.Short };
    private static final byte[] FIXED_2 = new byte[] { TypeId.Byte, TypeId.Int, TypeId.Double };
    private static final byte[] FIXED_3 = new byte[] { TypeId.Char, TypeId.Long, TypeId.Float };
    private static final byte[] MIXED_1 = new byte[] { TypeId.Short, TypeId.String, TypeId.Generic };
    private static final byte[] OBJ_1 = new byte[] { TypeId.Generic, TypeId.String, TypeId.String };

    static Stream<Arguments> variants() {
        return Stream.of(FIXED_1, FIXED_2, FIXED_3, MIXED_1, OBJ_1).map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("variants")
    void shouldInternRows(final byte[] fieldTypes) {
        final var resolver = resolver(fieldTypes);
        final var interner = new TupleInterner(List.of("F0", "F1", "F2"), 2);
        interner.bindToSchema(resolver);
        for(int i = 0; i < 5; i++) {
            assertThat(interner.intern(i), equalTo(interner.intern(i+5)));
        }
    }

    private FieldResolver resolver(final byte[] fieldTypes) {
        final Map<String, Field> fields = new HashMap<>();
        int salt = 1;
        for(byte typeId : fieldTypes) {
            final String name = "F" + fields.size();
            switch(typeId) {
<#list types as type>
               case TypeId.${type.name} -> fields.put(name, ${type.name?lower_case}Field(values(salt)));
</#list>
               default -> throw new IllegalArgumentException("Unhandled " + typeId);
            }
            salt++;
        }
        return fields::get;
    }

    private int[] values(final int salt) {
        return new int[] { salt, salt * 2, salt * 3, salt * 7, salt * 11,
                           salt, salt * 2, salt * 3, salt * 7, salt * 11};
    }

<#list types as type>
    private static Field ${type.name?lower_case}Field(final int... values) {
        final var field = ArrayFieldFactory.writable${type.name}ArrayField(16, 0, i -> {});
        for(int i = 0; i < values.length; i++) {
            field.setValueAt(i, ${type.name}Type.castTo${type.name}(values[i]));
        }
        return field;
    }
</#list>
}