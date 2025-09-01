// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.groupby;

import static com.bytefacets.spinel.exception.FieldNotFoundException.fieldNotFound;
import static com.bytefacets.spinel.schema.FieldList.fieldList;
import static com.bytefacets.spinel.schema.MappedFieldFactory.asMappedField;
import static com.bytefacets.spinel.schema.MatrixStoreFieldFactory.matrixStoreFieldFactory;
import static com.bytefacets.spinel.schema.Schema.schema;
import static com.bytefacets.spinel.schema.SchemaField.schemaField;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.hash.StringGenericIndexedMap;
import com.bytefacets.spinel.cache.Cache;
import com.bytefacets.spinel.cache.CacheBuilder;
import com.bytefacets.spinel.interner.RowInterner;
import com.bytefacets.spinel.schema.FieldBitSet;
import com.bytefacets.spinel.schema.FieldDescriptor;
import com.bytefacets.spinel.schema.MatrixStoreFieldFactory;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.SchemaField;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class GroupBySchemaBuilder {
    private final GroupFunctionBinding groupFunctionBinding = new GroupFunctionBinding();
    private final DependencyMap dependencyMap;
    private final String name;
    private final Collection<AggregationFunction> aggFunctions;
    private final Set<String> groupFieldNames;
    private final String groupFieldName;
    private final String countFieldName;
    private final StringGenericIndexedMap<SchemaField> fieldMap;
    private final CacheBuilder cacheBuilder;
    private final Cache cache;

    GroupBySchemaBuilder(
            final String name,
            @Nullable final String groupFieldName,
            @Nullable final String countFieldName,
            final int initialOutboundSize,
            final int chunkSize,
            final Collection<AggregationFunction> aggFunctions,
            final Set<String> groupFieldNames) {
        this.name = requireNonNull(name, "name");
        this.groupFieldName = groupFieldName;
        this.countFieldName = countFieldName;
        this.aggFunctions = requireNonNull(aggFunctions, "aggFunctions");
        this.groupFieldNames = requireNonNull(groupFieldNames, "groupFieldNames");
        this.fieldMap = new StringGenericIndexedMap<>(parentFieldCount(), 1f);
        this.cacheBuilder = CacheBuilder.cache();
        this.dependencyMap = new DependencyMap(aggFunctions.size());
        final FieldBitSet fieldTracker = dependencyMap.outboundFieldChangeSet();

        this.initializeFieldMap(
                new SummaryFieldAllocator(
                        cacheBuilder,
                        matrixStoreFieldFactory(
                                initialOutboundSize, chunkSize, fieldTracker::fieldChanged)));
        this.cache = cacheBuilder.build();
    }

    DependencyMap dependencyMap() {
        return dependencyMap;
    }

    GroupFunctionBinding groupFunctionBinding() {
        return groupFunctionBinding;
    }

    private void initializeFieldMap(final SummaryFieldAllocator summaryFieldAllocator) {
        if (groupFieldName != null) {
            fieldMap.add(groupFieldName);
        }
        groupFieldNames.forEach(fieldMap::add);
        if (countFieldName != null) {
            fieldMap.add(countFieldName);
        }
        aggFunctions.forEach(function -> function.collectFieldReferences(summaryFieldAllocator));
        summaryFieldAllocator.allocate();
    }

    private int parentFieldCount() {
        return (groupFieldName != null ? 1 : 0)
                + (countFieldName != null ? 1 : 0)
                + groupFieldNames.size()
                + aggFunctions.size()
                + 2; // accommodation for group by function fields
    }

    Schema buildParentSchema(
            final Schema inSchema,
            final RowInterner groupFunction,
            final GroupMapping groupMapping) {
        groupFunctionBinding.bind(inSchema, groupFunction);
        dependencyMap.reset();
        mapGroupFieldIfNecessary(groupMapping);
        mapCountFieldIfNecessary(groupMapping);
        mapGroupFields(inSchema, groupMapping);
        cache.bind(inSchema);
        final Schema outSchema = schema(name, fieldList(fieldMap));
        bindAggregateCalcs(inSchema, outSchema);
        return outSchema;
    }

    Cache cache() {
        return cache;
    }

    void unbindCalculatedFields() {
        aggFunctions.forEach(AggregationFunction::unbindSchema);
    }

    private void bindAggregateCalcs(final Schema inSchema, final Schema outboundSchema) {
        final var cacheResolver = cache.resolver();
        final var outResolver = outboundSchema.asFieldResolver();
        aggFunctions.forEach(
                func ->
                        func.bindToSchema(
                                cacheResolver,
                                dependencyMap.resolver(inSchema, func),
                                outResolver));
    }

    private void mapGroupFields(final Schema inSchema, final GroupMapping groupMapping) {
        groupFieldNames.forEach(fieldName -> mapGroupField(fieldName, inSchema, groupMapping));
        groupFunctionBinding
                .fieldNames()
                .forEach(
                        fieldName -> {
                            // don't add ones we've already added
                            if (!groupFieldNames.contains(fieldName)) {
                                mapGroupField(fieldName, inSchema, groupMapping);
                            }
                        });
    }

    private void mapGroupField(
            final String fieldName, final Schema inSchema, final GroupMapping groupMapping) {
        // explicitly forward fields will have been added to the fieldMap already
        // fields from the groupFunctionBinding will be at the end bc we're finding these out later
        final int id = fieldMap.add(fieldName);
        final SchemaField inField = inSchema.maybeField(fieldName);
        if (inField == null) {
            throw fieldNotFound(fieldName, inSchema.name());
        }
        final var outField = asMappedField(inField.field(), groupMapping.passThroughFieldMapper());
        fieldMap.putValueAt(id, schemaField(id, fieldName, outField, inField.metadata()));
        dependencyMap.mapInboundFieldIdToOutboundFieldId(inField.fieldId(), id);
    }

    private void mapGroupFieldIfNecessary(final GroupMapping groupMapping) {
        if (groupFieldName != null) {
            // should already be added to the fieldMap
            final int id = fieldMap.lookupEntry(groupFieldName);
            dependencyMap.setGroupIdFieldId(id);
            fieldMap.putValueAt(
                    id, schemaField(id, groupFieldName, groupMapping.parentGroupIdField()));
        }
    }

    private void mapCountFieldIfNecessary(final GroupMapping groupMapping) {
        if (countFieldName != null) {
            // should already be added to the fieldMap
            final int id = fieldMap.lookupEntry(countFieldName);
            dependencyMap.setGroupCountFieldId(id);
            fieldMap.putValueAt(id, schemaField(id, countFieldName, groupMapping.countField()));
        }
    }

    Collection<AggregationFunction> aggregationFunctions() {
        return aggFunctions;
    }

    private final class SummaryFieldAllocator implements AggregationSetupVisitor {
        private final MatrixStoreFieldFactory matrixFieldFactory;
        private final Map<Byte, List<FieldDescriptor>> typeMap = new HashMap<>();
        private final CacheBuilder cacheBuilder;

        private SummaryFieldAllocator(
                final CacheBuilder cacheBuilder, final MatrixStoreFieldFactory matrixFieldFactory) {
            this.cacheBuilder = requireNonNull(cacheBuilder, "cacheBuilder");
            this.matrixFieldFactory = requireNonNull(matrixFieldFactory, "matrixFieldFactory");
        }

        void allocate() {
            matrixFieldFactory.createFieldList(fieldMap, typeMap);
        }

        @Override
        public void addOutboundField(final FieldDescriptor outboundFieldDescriptor) {
            typeMap.computeIfAbsent(outboundFieldDescriptor.fieldType(), type -> new ArrayList<>(4))
                    .add(outboundFieldDescriptor);
        }

        @Override
        public void addPreviousValueField(final String fieldName) {
            cacheBuilder.cacheFields(fieldName);
        }
    }
}
