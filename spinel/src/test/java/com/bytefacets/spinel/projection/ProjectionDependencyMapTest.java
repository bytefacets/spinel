// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.projection;

import static com.bytefacets.spinel.projection.CalculatedField.asCalculatedField;
import static com.bytefacets.spinel.schema.FieldBitSet.fieldBitSet;
import static com.bytefacets.spinel.schema.FieldList.fieldList;
import static com.bytefacets.spinel.schema.Schema.schema;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.bytefacets.spinel.projection.lib.IntFieldCalculation;
import com.bytefacets.spinel.schema.Field;
import com.bytefacets.spinel.schema.FieldList;
import com.bytefacets.spinel.schema.FieldResolver;
import com.bytefacets.spinel.schema.IntField;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ProjectionDependencyMapTest {
    private final ProjectionDependencyMap dependencyMap = new ProjectionDependencyMap();
    private final BitSet result = new BitSet();

    @Test
    void shouldTranslateMappedFieldChanges() {
        dependencyMap.mapInboundFieldIdToOutboundFieldId(2, 3);
        dependencyMap.mapInboundFieldIdToOutboundFieldId(6, 10);
        dependencyMap.translateInboundChangeFields(fieldBitSet(setOf(1, 2, 3, 4, 5, 6, 7)), result);
        assertThat(result, equalTo(setOf(3, 10)));
        assertThat(dependencyMap.referenceCount(), equalTo(2));
    }

    @Test
    void shouldResetReferences() {
        IntStream.range(0, 11)
                .forEach(i -> dependencyMap.mapInboundFieldIdToOutboundFieldId(i, i + 1));
        assertThat(dependencyMap.referenceCount(), equalTo(11));
        dependencyMap.reset();
        assertThat(dependencyMap.referenceCount(), equalTo(0));
    }

    @Nested
    class CalculatedFieldTests {
        private final IntFieldCalculation calc1 = mock(IntFieldCalculation.class);
        private final IntFieldCalculation calc2 = mock(IntFieldCalculation.class);
        private final FieldList inFields = fields("a", "b");
        private Map<String, FieldCalculation> calcs = Map.of("calc1", calc1, "calc2", calc2);
        private FieldList outFields;

        private void bind() {
            dependencyMap.bindCalculatedFields(
                    schema("", inFields), schema("", outFields), toDescriptors(calcs));
        }

        private void buildOutFields() {
            final Map<String, Field> map = new LinkedHashMap<>();
            map.put("a", inFields.field("a").field());
            calcs.forEach((name, calc) -> map.put(name, asCalculatedField(calc)));
            outFields = fieldList(map);
            dependencyMap.mapInboundFieldIdToOutboundFieldId(
                    inFields.field("a").fieldId(), outFields.field("a").fieldId());
        }

        @Test
        void shouldUnbindCalculatedFields() {
            calcs = Map.of("calc1", calc1, "calc2", calc2);
            dependencyMap.unbindCalculatedFields(toDescriptors(calcs));

            verify(calcs.get("calc1"), times(1)).unbindSchema();
            verify(calcs.get("calc2"), times(1)).unbindSchema();
        }

        @Test
        void shouldBindCalculatedFields() {
            calcs = Map.of("calc1", calc1, "calc2", calc2);
            buildOutFields();
            bind();

            verify(calcs.get("calc1"), times(1)).bindToSchema(any());
            verify(calcs.get("calc2"), times(1)).bindToSchema(any());
        }

        @Test
        void shouldMapCalculatedFieldDependencies() {
            calcs = Map.of("calc1", calc("a", "b"), "calc2", calc("b"));
            buildOutFields();
            bind();
            final int a = inFields.field("a").fieldId();
            final int b = inFields.field("b").fieldId();
            final int fCalc1 = outFields.field("calc1").fieldId();
            final int fCalc2 = outFields.field("calc2").fieldId();

            dependencyMap.translateInboundChangeFields(fieldBitSet(setOf(b)), result);
            assertThat(result, equalTo(setOf(fCalc1, fCalc2)));

            result.clear();

            dependencyMap.translateInboundChangeFields(fieldBitSet(setOf(a)), result);
            assertThat(result, equalTo(setOf(a, fCalc1)));
        }
    }

    private Map<String, CalculatedFieldDescriptor> toDescriptors(
            final Map<String, FieldCalculation> calcs) {
        final Map<String, CalculatedFieldDescriptor> result = new LinkedHashMap<>();
        calcs.forEach(
                (name, calc) -> result.put(name, new CalculatedFieldDescriptor(name, calc, null)));
        return result;
    }

    private IntFieldCalculation calc(final String... dependencies) {
        return new IntFieldCalculation() {
            @Override
            public int calculate(final int row) {
                return 0;
            }

            @Override
            public void bindToSchema(final FieldResolver fieldResolver) {
                Stream.of(dependencies).forEach(fieldResolver::findField);
            }

            @Override
            public void unbindSchema() {}
        };
    }

    private FieldList fields(final String... names) {
        final Map<String, Field> map = new LinkedHashMap<>();
        Stream.of(names).forEach(name -> map.put(name, field()));
        return fieldList(map);
    }

    private IntField field() {
        return mock(IntField.class);
    }

    private BitSet setOf(final int... ids) {
        final BitSet set = new BitSet();
        IntStream.of(ids).forEach(set::set);
        return set;
    }
}
