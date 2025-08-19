<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.groupby.lib;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.collections.types.${type.name}Type;
import com.bytefacets.spinel.groupby.AggregationFunction;
import com.bytefacets.spinel.groupby.AggregationSetupVisitor;
import com.bytefacets.spinel.schema.FieldResolver;
import com.bytefacets.spinel.schema.Metadata;
import com.bytefacets.spinel.schema.${type.name}Field;
import com.bytefacets.spinel.schema.${type.name}WritableField;
import com.bytefacets.spinel.schema.FieldDescriptor;
import com.bytefacets.spinel.schema.TypeId;

import java.util.Map;
import java.util.Objects;

public final class ${type.name}Aggregation implements AggregationFunction {
    private static final ${type.arrayType} NO_VALUE = ${type.name}Type.DEFAULT;
    private final String inboundFieldName;
    private final FieldDescriptor outboundFieldDescriptor;
    private final ${type.name}Accumulator accumulator;
    private ${type.name}Field newValueField;
    private ${type.name}Field oldValueField;
    private ${type.name}WritableField outboundField;

    public static ${type.name}Aggregation ${type.name?lower_case}Aggregation(
            final String inboundFieldName,
            final String outboundFieldName,
            final ${type.name}Accumulator accumulator) {
        return new ${type.name}Aggregation(inboundFieldName, outboundFieldName, accumulator, Metadata.EMPTY);
    }

    public static ${type.name}Aggregation ${type.name?lower_case}Aggregation(
            final String inboundFieldName,
            final String outboundFieldName,
            final ${type.name}Accumulator accumulator,
            final Metadata metadata) {
        return new ${type.name}Aggregation(inboundFieldName, outboundFieldName, accumulator, metadata);
    }

    private ${type.name}Aggregation(
            final String inboundFieldName,
            final String outboundFieldName,
            final ${type.name}Accumulator accumulator,
            final Metadata metadata) {
        this.inboundFieldName = Objects.requireNonNull(inboundFieldName, "inboundFieldName");
        this.outboundFieldDescriptor = new FieldDescriptor(TypeId.${type.name}, outboundFieldName, metadata);
        this.accumulator = Objects.requireNonNull(accumulator, "accumulator");
    }

    @Override
    public void collectFieldReferences(final AggregationSetupVisitor visitor) {
        visitor.addPreviousValueField(inboundFieldName);
        visitor.addInboundField(inboundFieldName);
        visitor.addOutboundField(outboundFieldDescriptor);
    }

    @Override
    public void groupRowsAdded(final int group, final IntIterable rows) {
        rows.forEach(row -> {
            final ${type.arrayType} newValue = newValueField.valueAt(row);
            accumulate(group, NO_VALUE, newValue);
        });
    }

    @Override
    public void groupRowsChanged(final int group, final IntIterable rows) {
        rows.forEach(row -> {
            final ${type.arrayType} newValue = newValueField.valueAt(row);
            final ${type.arrayType} oldValue = oldValueField.valueAt(row);
            accumulate(group, oldValue, newValue);
        });
    }

    @Override
    public void groupRowsRemoved(final int group, final IntIterable rows) {
        rows.forEach(row -> {
            final ${type.arrayType} oldValue = oldValueField.valueAt(row);
            accumulate(group, oldValue, NO_VALUE);
        });
    }

    @Override
    public void bindToSchema(final FieldResolver previousResolver, final FieldResolver currentResolver, final FieldResolver outboundResolver) {
        this.newValueField = currentResolver.find${type.name}Field(inboundFieldName);
        this.oldValueField = previousResolver.find${type.name}Field(inboundFieldName);
        this.outboundField = (${type.name}WritableField) outboundResolver.find${type.name}Field(outboundFieldDescriptor.name());
    }

    @Override
    public void unbindSchema() {
        newValueField = null;
        oldValueField = null;
        outboundField = null;
    }

    private void accumulate(final int group, final ${type.arrayType} oldValue, final ${type.arrayType} newValue) {
        final ${type.arrayType} oldSummary = outboundField.valueAt(group);
        final ${type.arrayType} newSummary = accumulator.accumulate(oldSummary, oldValue, newValue);
        if(!${type.name}Type.EqImpl.areEqual(oldSummary, newSummary)) {
            outboundField.setValueAt(group, newSummary);
        }
    }

    public interface ${type.name}Accumulator {
        ${type.arrayType} accumulate(${type.arrayType} currentGroupValue, ${type.arrayType} oldValue, ${type.arrayType} newValue);
    }
}
