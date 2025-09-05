// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.jdbc.source;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.hash.StringGenericIndexedMap;
import com.bytefacets.spinel.schema.FieldDescriptor;
import com.bytefacets.spinel.schema.MatrixStoreFieldFactory;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.SchemaField;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class JdbcSourceSchemaBuilder {
    private final String name;
    private final JdbcSourceBindingProvider bindingProvider;
    private final MatrixStoreFieldFactory fieldFactory;
    private final List<ResultSetBinding> bindings = new ArrayList<>();
    private final JdbcToFieldNamer jdbcToFieldNamer;

    JdbcSourceSchemaBuilder(
            final String name,
            final JdbcSourceBindingProvider bindingProvider,
            final JdbcToFieldNamer jdbcToFieldNamer,
            final MatrixStoreFieldFactory fieldFactory) {
        this.name = requireNonNull(name, "name");
        this.bindingProvider = requireNonNull(bindingProvider, "bindingProvider");
        this.fieldFactory = requireNonNull(fieldFactory, "fieldFactory");
        this.jdbcToFieldNamer = requireNonNull(jdbcToFieldNamer, "jdbcToFieldNamer");
    }

    List<ResultSetBinding> bindings() {
        return bindings;
    }

    // VisibleForTesting
    Schema createSchema(final ResultSetMetaData metaData) throws Exception {
        final StringGenericIndexedMap<SchemaField> fieldMap = new StringGenericIndexedMap<>(10);
        final Map<Byte, List<FieldDescriptor>> typeMap = new HashMap<>();
        for (int i = 1, len = metaData.getColumnCount(); i <= len; i++) {
            final String jdbcFieldName = metaData.getColumnName(i);
            final String fieldName = jdbcToFieldNamer.jdbcToFieldName(jdbcFieldName);
            final int sqlType = metaData.getColumnType(i);
            final JdbcFieldBinding binding = bindingProvider.create(fieldName, sqlType);
            final FieldDescriptor descriptor = binding.createDescriptor(fieldName);
            fieldMap.add(descriptor.name());
            typeMap.computeIfAbsent(descriptor.fieldType(), k -> new ArrayList<>(4))
                    .add(descriptor);
            bindings.add(new ResultSetBinding(i, binding, descriptor.name()));
        }
        final Schema schema = Schema.schema(name, fieldFactory.createFieldList(fieldMap, typeMap));
        bindings.forEach(binding -> binding.bindToSchema(schema.asFieldResolver()));
        return schema;
    }
}
