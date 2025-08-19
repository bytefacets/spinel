// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.groupby;

import static com.bytefacets.spinel.common.DefaultNameSupplier.resolveName;
import static com.bytefacets.spinel.exception.OperatorSetupException.setupException;
import static com.bytefacets.spinel.transform.BuilderSupport.builderSupport;
import static com.bytefacets.spinel.transform.TransformContext.continuation;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

import com.bytefacets.spinel.schema.FieldDescriptor;
import com.bytefacets.spinel.transform.BuilderSupport;
import com.bytefacets.spinel.transform.TransformContext;
import com.bytefacets.spinel.transform.TransformContinuation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public final class GroupByBuilder {
    private final TransformContext transformContext;
    private final List<String> forwardedFields = new ArrayList<>(2);
    private final List<AggregationFunction> aggFunctions = new ArrayList<>(2);
    private final Set<String> outboundAggFieldNames = new HashSet<>(2);
    private final OutboundFieldNameCollector fieldNameCollector = new OutboundFieldNameCollector();
    private final String name;
    private final BuilderSupport<GroupBy> builderSupport;
    private GroupFunction groupFunction;
    private String groupIdFieldName;
    private String countFieldName;
    private int initialOutboundSize = 128;
    private int initialInboundSize = 128;
    private int chunkSize = 128;

    private GroupByBuilder(final String name) {
        this.name = requireNonNull(name, "name");
        this.builderSupport = builderSupport(name, this::internalBuild);
        this.transformContext = null;
    }

    private GroupByBuilder(final TransformContext context) {
        this.transformContext = requireNonNull(context, "transform context");
        this.name = context.name();
        this.builderSupport =
                context.createBuilderSupport(this::internalBuild, () -> getOrCreate().input());
    }

    public static GroupByBuilder groupBy() {
        return groupBy((String) null);
    }

    public static GroupByBuilder groupBy(final String name) {
        return new GroupByBuilder(resolveName("GroupBy", name));
    }

    public static GroupByBuilder groupBy(final TransformContext transformContext) {
        return new GroupByBuilder(transformContext);
    }

    public GroupBy getOrCreate() {
        return builderSupport.getOrCreate();
    }

    public TransformContinuation thenWithParentOutput() {
        return continuation(
                transformContext,
                builderSupport.transformNode(),
                () -> getOrCreate().parentOutput());
    }

    public TransformContinuation thenWithChildOutput() {
        return continuation(
                transformContext,
                builderSupport.transformNode(),
                () -> getOrCreate().childOutput());
    }

    private GroupBy internalBuild() {
        builderSupport.throwIfBuilt();
        return new GroupBy(
                schemaBuilder(),
                childSchemaBuilder(),
                groupFunction,
                initialOutboundSize,
                initialInboundSize);
    }

    private ChildSchemaBuilder childSchemaBuilder() {
        return new ChildSchemaBuilder(name + ".child", groupIdFieldName);
    }

    public GroupBy build() {
        return builderSupport.createOperator();
    }

    public GroupByBuilder groupFunction(final GroupFunction groupFunction) {
        this.groupFunction = requireNonNull(groupFunction, "groupFunction");
        return this;
    }

    public GroupByBuilder includeGroupIdField(final String groupIdFieldName) {
        NameType.groupId.throwIfCollides(name, groupIdFieldName, NameType.count, countFieldName);
        NameType.groupId.throwIfCollides(
                name, groupIdFieldName, NameType.forwarded, forwardedFields);
        NameType.groupId.throwIfCollides(
                name, groupIdFieldName, NameType.calculated, outboundAggFieldNames);
        this.groupIdFieldName = requireNonNull(groupIdFieldName, "groupIdFieldName");
        return this;
    }

    public GroupByBuilder includeCountField(final String countFieldName) {
        NameType.count.throwIfCollides(name, countFieldName, NameType.groupId, groupIdFieldName);
        NameType.count.throwIfCollides(name, countFieldName, NameType.forwarded, forwardedFields);
        NameType.count.throwIfCollides(
                name, countFieldName, NameType.calculated, outboundAggFieldNames);
        this.countFieldName = requireNonNull(countFieldName, "countFieldName");
        return this;
    }

    public GroupByBuilder addForwardedFields(final String... forwardedFields) {
        for (String f : forwardedFields) {
            NameType.forwarded.throwIfCollides(name, f, NameType.groupId, groupIdFieldName);
            NameType.forwarded.throwIfCollides(name, f, NameType.count, countFieldName);
            NameType.forwarded.throwIfCollides(name, f, NameType.calculated, outboundAggFieldNames);
            this.forwardedFields.add(f);
        }
        return this;
    }

    public GroupByBuilder addAggregation(final AggregationFunction function) {
        function.collectFieldReferences(fieldNameCollector);
        aggFunctions.add(function);
        return this;
    }

    public GroupByBuilder initialOutboundSize(final int initialOutboundSize) {
        this.initialOutboundSize = initialOutboundSize;
        return this;
    }

    public GroupByBuilder initialInboundSize(final int initialInboundSize) {
        this.initialInboundSize = initialInboundSize;
        return this;
    }

    public GroupByBuilder chunkSize(final int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    GroupBySchemaBuilder schemaBuilder() {
        if (groupIdFieldName == null
                && countFieldName == null
                && outboundAggFieldNames.isEmpty()
                && forwardedFields.isEmpty()) {
            throw setupException(
                    "No fields will be in schema; all field sources are empty for "
                            + requireNonNullElse(name, "new GroupBy"));
        }
        return new GroupBySchemaBuilder(
                name,
                groupIdFieldName,
                countFieldName,
                initialOutboundSize,
                chunkSize,
                aggFunctions,
                forwardedFields);
    }

    private final class OutboundFieldNameCollector implements AggregationSetupVisitor {
        @Override
        public void addOutboundField(final FieldDescriptor outboundFieldDescriptor) {
            final String aggFieldName = outboundFieldDescriptor.name();
            NameType.calculated.throwIfCollides(
                    name, aggFieldName, NameType.groupId, groupIdFieldName);
            NameType.calculated.throwIfCollides(name, aggFieldName, NameType.count, countFieldName);
            NameType.calculated.throwIfCollides(
                    name, aggFieldName, NameType.forwarded, forwardedFields);
            outboundAggFieldNames.add(aggFieldName);
        }
    }

    private enum NameType {
        groupId("GroupIdFieldName"),
        count("CountFieldName"),
        forwarded("ForwardedField"),
        calculated("CalculatedField");
        private final String type;

        NameType(final String type) {
            this.type = type;
        }

        void throwIfCollides(
                final String name,
                final String request,
                final NameType existingType,
                final Collection<String> existingCollection) {
            existingCollection.forEach(
                    existing -> throwIfCollides(name, request, existingType, existing));
        }

        void throwIfCollides(
                final String name,
                final String request,
                final NameType existingType,
                final String existing) {
            if (Objects.equals(existing, request)) {
                final String operatorName = Objects.requireNonNullElse(name, "new GroupBy");
                throw setupException(
                        String.format(
                                "Name collision setting up %s: attempted to use '%s' for %s, but was already used for %s",
                                operatorName, request, this.type, existingType));
            }
        }

        @Override
        public String toString() {
            return type;
        }
    }
}
