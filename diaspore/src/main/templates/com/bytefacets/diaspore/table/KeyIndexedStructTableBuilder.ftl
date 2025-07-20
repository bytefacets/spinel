<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.table;

import static com.bytefacets.diaspore.schema.MatrixStoreFieldFactory.matrixStoreFieldFactory;
import static com.bytefacets.diaspore.schema.Schema.schema;

import com.bytefacets.collections.hash.*;
import com.bytefacets.diaspore.exception.OperatorSetupException;
import com.bytefacets.diaspore.facade.StructFacadeFactory;
import com.bytefacets.diaspore.facade.StructFieldExtractor;
import com.bytefacets.diaspore.schema.Field;
import com.bytefacets.diaspore.schema.FieldDescriptor;
import com.bytefacets.diaspore.schema.FieldList;
import com.bytefacets.diaspore.schema.IndexedSetFieldFactory;
import com.bytefacets.diaspore.schema.MatrixStoreFieldFactory;
import com.bytefacets.diaspore.schema.SchemaField;
import com.bytefacets.diaspore.schema.TypeId;
import com.bytefacets.diaspore.transform.TransformContext;
import com.bytefacets.diaspore.transform.TransformContinuation;
import com.bytefacets.diaspore.transform.BuilderSupport;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

<#if type.name == "Generic">
    <#assign classGenerics="<T, S>">
<#else>
    <#assign classGenerics="<S>">
</#if>
public final class ${type.name}IndexedStructTableBuilder${classGenerics} {
    private final BuilderSupport<${type.name}IndexedStructTable${classGenerics}> builderSupport;
    private final TransformContext transformContext;
    private final Map<Byte, List<FieldDescriptor>> typeMap = new HashMap<>(TypeId.Max + 1, 1);
    private final StringGenericIndexedMap<SchemaField> fieldMap = new StringGenericIndexedMap<>(16);
    private final Class<S> structType;
    private String keyFieldName;
    private int initialSize = 64;
    private int chunkSize = 64;

    private ${type.name}IndexedStructTableBuilder(final Class<S> structType) {
        this.structType = Objects.requireNonNull(structType, "structType");
        StructFieldExtractor.consumeFields(structType, this::captureWritableField, this::captureReadOnlyField);
        final String name = structType.getSimpleName();
        this.builderSupport = BuilderSupport.builderSupport(name, this::internalBuild);
        this.transformContext = null;
    }

    private void captureWritableField(final FieldDescriptor fd) {
        fieldMap.add(fd.name());
        typeMap.computeIfAbsent(fd.fieldType(), ArrayList::new).add(fd);
    }

    private void captureReadOnlyField(final FieldDescriptor fd) {
        if(keyFieldName != null) {
            final var msg = String.format("Found at least two read-only fields in %s, but " +
                                          "can only accommodate one: %s and %s",
                                          structType.getName(), keyFieldName, fd.name());
            throw new OperatorSetupException(msg);
        }
        keyFieldName = fd.name();
    }

    private ${type.name}IndexedStructTableBuilder(final Class<S> structType, final TransformContext transformContext) {
        this.structType = Objects.requireNonNull(structType, "structType");
        this.transformContext = Objects.requireNonNull(transformContext, "transform context");
        StructFieldExtractor.consumeFields(structType, this::captureWritableField, this::captureReadOnlyField);
        this.builderSupport = transformContext.createBuilderSupport(this::internalBuild, null);
    }

    public ${type.name}IndexedStructTableBuilder${classGenerics} initialSize(final int initialSize) {
        this.initialSize = initialSize;
        return this;
    }

    public ${type.name}IndexedStructTableBuilder${classGenerics} chunkSize(final int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    public ${type.name}IndexedStructTable${classGenerics} getOrCreate() {
        return builderSupport.getOrCreate();
    }

    public ${type.name}IndexedStructTable${classGenerics} build() {
        return builderSupport.createOperator();
    }

    public TransformContinuation then() {
        return TransformContext.continuation(transformContext, builderSupport.transformNode(), () -> getOrCreate().output());
    }

    public static ${classGenerics} ${type.name}IndexedStructTableBuilder${classGenerics} ${type.name?lower_case}IndexedStructTable(final Class<S> structType) {
        return new ${type.name}IndexedStructTableBuilder${classGenerics}(structType);
    }

    public static ${classGenerics} ${type.name}IndexedStructTableBuilder${classGenerics} ${type.name?lower_case}IndexedStructTable(final Class<S> structType, final TransformContext transformContext) {
        return new ${type.name}IndexedStructTableBuilder${classGenerics}(structType, transformContext);
    }

    private ${type.name}IndexedStructTable${classGenerics} internalBuild() {
        final var index = new ${type.name}IndexedSet${generics}(initialSize);
        if(keyFieldName != null) {
            final int fieldId = fieldMap.add(keyFieldName);
            final Field field = IndexedSetFieldFactory.asKeyField(index);
            fieldMap.putValueAt(fieldId, SchemaField.schemaField(fieldId, keyFieldName, field));
        }

        final TableStateChange change = new TableStateChange();
        final MatrixStoreFieldFactory fieldFactory =
                matrixStoreFieldFactory(initialSize, chunkSize, change.fieldChangeListener());
        final FieldList fieldList = fieldFactory.createFieldList(fieldMap, typeMap);
        final String name = structType.getSimpleName();
        final var facadeFactory = StructFacadeFactory.structFacadeFactory();
        return new ${type.name}IndexedStructTable<>(index, schema(name, fieldList), structType, change, facadeFactory);
    }
}