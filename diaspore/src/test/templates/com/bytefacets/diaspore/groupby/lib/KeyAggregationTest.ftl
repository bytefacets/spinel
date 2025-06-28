<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.groupby.lib;

import com.bytefacets.collections.types.${type.name}Type;
import com.bytefacets.collections.vector.IntVector;
import com.bytefacets.diaspore.groupby.AggregationSetupVisitor;
import com.bytefacets.diaspore.schema.ArrayFieldFactory;
import com.bytefacets.diaspore.schema.${type.name}WritableField;
import com.bytefacets.diaspore.schema.FieldDescriptor;
import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class ${type.name}AggregationTest {
    private final ${type.name}Aggregation agg = ${type.name}Aggregation.${type.name?lower_case}Aggregation("value", "agg", new ${type.name}Aggregation.${type.name}Accumulator() {
        @Override
        public ${type.arrayType} accumulate(final ${type.arrayType} currentGroupValue, final ${type.arrayType} oldValue, final ${type.arrayType} newValue) {
<#if type.name == "Generic" || type.name == "String">
            final int curV = currentGroupValue != null ? ${type.name}Type.castToInt(currentGroupValue) : 0;
            final int oldV = oldValue != null ? ${type.name}Type.castToInt(oldValue) : 0;
            final int newV = newValue != null ? ${type.name}Type.castToInt(newValue) : 0;
<#else>
            final int curV = ${type.name}Type.castToInt(currentGroupValue);
            final int oldV = ${type.name}Type.castToInt(oldValue);
            final int newV = ${type.name}Type.castToInt(newValue);
</#if>
            return v(curV - oldV + newV);
        }
    });
    private final ${type.name}WritableField inField = ArrayFieldFactory.writable${type.name}ArrayField(10, 0, i -> {});
    private final ${type.name}WritableField prevField = ArrayFieldFactory.writable${type.name}ArrayField(10, 0, i -> {});
    private final ${type.name}WritableField groupField = ArrayFieldFactory.writable${type.name}ArrayField(10, 0, i -> {});

    @Test
    void shouldCollectFieldNames() {
        final AggregationSetupVisitor visitor = mock(AggregationSetupVisitor.class);
        agg.collectFieldReferences(visitor);
        verify(visitor, times(1)).addInboundField("value");
        verify(visitor, times(1)).addOutboundField(FieldDescriptor.${type.name?lower_case}Field("agg"));
        verify(visitor, times(1)).addPreviousValueField("value");
    }

    @Test
    void shouldAccumulateAdded() {
        IntStream.range(0, 10).forEach(i -> inField.setValueAt(i, v(i+10)));
        bind();
        agg.groupRowsAdded(5, rows(2, 4, 9));
        assertThat(groupField.valueAt(5), equalTo(v(12 + 14 + 19)));
    }

    @Test
    void shouldAccumulateChanged() {
        IntStream.range(0, 10).forEach(i -> inField.setValueAt(i, v(i+10)));
        IntStream.range(0, 10).forEach(i -> prevField.setValueAt(i, v(i+15)));
        groupField.setValueAt(5, v(100));
        bind();
        agg.groupRowsChanged(5, rows(2, 4, 9));
        assertThat(groupField.valueAt(5), equalTo(v(100-15)));
    }

    @Test
    void shouldAccumulateRemoved() {
        IntStream.range(0, 10).forEach(i -> inField.setValueAt(i, v(i+10)));
        IntStream.range(0, 10).forEach(i -> prevField.setValueAt(i, v(i+15)));
        groupField.setValueAt(5, v(100));
        bind();
        agg.groupRowsRemoved(5, rows(2, 4, 9));
<#if type.name == "Bool">
        assertThat(groupField.valueAt(5), equalTo(false));
<#else>
        assertThat(groupField.valueAt(5), equalTo(v(100-60)));
</#if>
    }

    private IntVector rows(final int... rows) {
        final IntVector vector = new IntVector(3);
        IntStream.of(rows).forEach(vector::append);
        return vector;
    }

    private void bind() {
        agg.bindToSchema(name -> prevField, name -> inField, name -> groupField);
    }

    private static ${type.arrayType} v(final int value) {
        return ${type.name}Type.castTo${type.name}(value);
    }
}