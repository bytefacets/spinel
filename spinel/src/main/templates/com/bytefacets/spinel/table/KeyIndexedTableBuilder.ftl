<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.table;

import static com.bytefacets.spinel.common.DefaultNameSupplier.resolveName;
import static com.bytefacets.spinel.schema.MatrixStoreFieldFactory.matrixStoreFieldFactory;
import static com.bytefacets.spinel.schema.Schema.schema;
import static java.util.Objects.requireNonNullElseGet;

import com.bytefacets.collections.hash.*;
import com.bytefacets.spinel.schema.Field;
import com.bytefacets.spinel.schema.FieldDescriptor;
import com.bytefacets.spinel.schema.FieldList;
import com.bytefacets.spinel.schema.IndexedSetFieldFactory;
import com.bytefacets.spinel.schema.MatrixStoreFieldFactory;
import com.bytefacets.spinel.schema.SchemaField;
import com.bytefacets.spinel.schema.TypeId;
import com.bytefacets.spinel.transform.TransformContext;
import com.bytefacets.spinel.transform.TransformContinuation;
import com.bytefacets.spinel.transform.BuilderSupport;

import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class ${type.name}IndexedTableBuilder${generics} {
    private final BuilderSupport<${type.name}IndexedTable${generics}> builderSupport;
    private final TransformContext transformContext;
    private final Set<String> names = new HashSet<>();
    private final Map<Byte, List<FieldDescriptor>> fieldMap = new HashMap<>(TypeId.Max + 1, 1);
    private final String name;
    private int initialSize = 64;
    private int chunkSize = 64;
    private int fieldCount;
    private boolean includeKeyField = true;
    private String keyFieldName;

    private ${type.name}IndexedTableBuilder(final String name) {
        this.name = Objects.requireNonNull(name, "name");
        this.builderSupport = BuilderSupport.builderSupport(this.name, this::internalBuild);
        this.transformContext = null;
    }

    private ${type.name}IndexedTableBuilder(final TransformContext transformContext) {
        this.transformContext = Objects.requireNonNull(transformContext, "transform context");
        this.name = transformContext.name();
        this.builderSupport = transformContext.createBuilderSupport(this::internalBuild, null);
    }

    public ${type.name}IndexedTableBuilder${generics} initialSize(final int initialSize) {
        this.initialSize = initialSize;
        return this;
    }

    public ${type.name}IndexedTableBuilder${generics} chunkSize(final int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    public ${type.name}IndexedTableBuilder${generics} includeKeyField(final boolean includeKeyField) {
        this.includeKeyField = includeKeyField;
        return this;
    }

    public ${type.name}IndexedTableBuilder${generics} keyFieldName(final String keyFieldName) {
        this.keyFieldName = keyFieldName;
        return this;
    }

    public ${type.name}IndexedTableBuilder${generics} addFields(final FieldDescriptor... fieldReference) {
        for(var field : fieldReference) {
            addField(field);
        }
        return this;
    }

    public ${type.name}IndexedTableBuilder${generics} addField(final FieldDescriptor fieldReference) {
        if (names.add(fieldReference.name())) {
            fieldMap.computeIfAbsent(fieldReference.fieldType(), ArrayList::new)
                    .add(fieldReference);
            fieldCount++;
        } else {
            throw new IllegalArgumentException("Duplicate field name: " + fieldReference.name());
        }
        return this;
    }

    private String createKeyFieldName(final String tableName) {
        return Objects.requireNonNullElse(keyFieldName, tableName + "Key");
    }

    private int fieldCount() {
        return fieldCount + (includeKeyField ? 1 : 0);
    }

    public ${type.name}IndexedTable${generics} getOrCreate() {
        return builderSupport.getOrCreate();
    }

    public ${type.name}IndexedTable${generics} build() {
        return builderSupport.createOperator();
    }

    public TransformContinuation then() {
        return TransformContext.continuation(transformContext, builderSupport.transformNode(), () -> getOrCreate().output());
    }

    public static ${generics} ${type.name}IndexedTableBuilder${generics} ${type.name?lower_case}IndexedTable() {
        return ${type.name?lower_case}IndexedTable((String)null);
    }

    public static ${generics} ${type.name}IndexedTableBuilder${generics} ${type.name?lower_case}IndexedTable(final String name) {
        return new ${type.name}IndexedTableBuilder${generics}(resolveName("${type.name}IndexedTable", name));
    }

    public static ${generics} ${type.name}IndexedTableBuilder${generics} ${type.name?lower_case}IndexedTable(final TransformContext transformContext) {
        return new ${type.name}IndexedTableBuilder${generics}(transformContext);
    }

    private ${type.name}IndexedTable${generics} internalBuild() {
        final StringGenericIndexedMap<SchemaField> fields = new StringGenericIndexedMap<>(fieldCount(), 1f);

        final var index = new ${type.name}IndexedSet${generics}(initialSize);
        if(includeKeyField) {
            final String fieldName = createKeyFieldName(name);
            final int fieldId = fields.add(fieldName);
            final Field field = IndexedSetFieldFactory.asKeyField(index);
            fields.putValueAt(fieldId, SchemaField.schemaField(fieldId, fieldName, field));
        }

        final TableStateChange change = new TableStateChange();
        final MatrixStoreFieldFactory fieldFactory =
                matrixStoreFieldFactory(initialSize, chunkSize, change.fieldChangeListener());
        final FieldList fieldList = fieldFactory.createFieldList(fields, fieldMap);
        return new ${type.name}IndexedTable${generics}(index, schema(name, fieldList), change);
    }
}