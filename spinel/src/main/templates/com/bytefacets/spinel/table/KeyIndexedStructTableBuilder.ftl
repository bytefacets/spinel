<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.table;

import static com.bytefacets.spinel.exception.FieldNotFoundException.fieldNotFound;
import static com.bytefacets.spinel.schema.MatrixStoreFieldFactory.matrixStoreFieldFactory;
import static com.bytefacets.spinel.schema.Schema.schema;

import com.bytefacets.collections.hash.*;
import com.bytefacets.spinel.exception.OperatorSetupException;
import com.bytefacets.spinel.facade.FieldNamingStrategy;
import com.bytefacets.spinel.facade.StructFacadeFactory;
import com.bytefacets.spinel.facade.StructFieldExtractor;
import com.bytefacets.spinel.schema.Field;
import com.bytefacets.spinel.schema.FieldDescriptor;
import com.bytefacets.spinel.schema.FieldList;
import com.bytefacets.spinel.schema.IndexedSetFieldFactory;
import com.bytefacets.spinel.schema.MatrixStoreFieldFactory;
import com.bytefacets.spinel.schema.Metadata;
import com.bytefacets.spinel.schema.SchemaField;
import com.bytefacets.spinel.schema.TypeId;
import com.bytefacets.spinel.transform.TransformContext;
import com.bytefacets.spinel.transform.TransformContinuation;
import com.bytefacets.spinel.transform.BuilderSupport;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

<#if type.name == "Generic">
    <#assign classGenerics="<T, S>">
<#else>
    <#assign classGenerics="<S>">
</#if>
public final class ${type.name}IndexedStructTableBuilder${classGenerics} {
    private final BuilderSupport<${type.name}IndexedStructTable${classGenerics}> builderSupport;
    private final TransformContext transformContext;
    private final List<FieldDescriptor> writableFields = new ArrayList<>(8);
    private final Class<S> structType;
    private final String name;
    private FieldNamingStrategy fieldNamingStrategy = FieldNamingStrategy.Identity;
    private FieldDescriptor keyField;
    private int initialSize = 64;
    private int chunkSize = 64;

    private ${type.name}IndexedStructTableBuilder(final String name, final Class<S> structType) {
        this.structType = Objects.requireNonNull(structType, "structType");
        StructFieldExtractor.consumeFields(structType, this::captureWritableField, this::captureReadOnlyField);
        this.name = Objects.requireNonNull(name, "name");
        this.builderSupport = BuilderSupport.builderSupport(name, this::internalBuild);
        this.transformContext = null;
    }

    private void captureWritableField(final FieldDescriptor fd) {
        writableFields.add(fd);
    }

    private void captureReadOnlyField(final FieldDescriptor fd) {
        if(keyField != null) {
            final var msg = String.format("Found at least two read-only fields in %s, but " +
                                          "can only accommodate one: %s and %s",
                                          structType.getName(), keyField.name(), fd.name());
            throw new OperatorSetupException(msg);
        }
        keyField = fd;
    }

    private ${type.name}IndexedStructTableBuilder(final String name, final Class<S> structType, final TransformContext transformContext) {
        this.structType = Objects.requireNonNull(structType, "structType");
        this.name = Objects.requireNonNull(name, "name");
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
        return new ${type.name}IndexedStructTableBuilder${classGenerics}(structType.getSimpleName(), structType);
    }

    public static ${classGenerics} ${type.name}IndexedStructTableBuilder${classGenerics} ${type.name?lower_case}IndexedStructTable(final Class<S> structType, final TransformContext transformContext) {
        return new ${type.name}IndexedStructTableBuilder${classGenerics}(structType.getSimpleName(), structType, transformContext);
    }

    public static ${classGenerics} ${type.name}IndexedStructTableBuilder${classGenerics} ${type.name?lower_case}IndexedStructTable(final String name, final Class<S> structType) {
        return new ${type.name}IndexedStructTableBuilder${classGenerics}(name, structType);
    }

    public static ${classGenerics} ${type.name}IndexedStructTableBuilder${classGenerics} ${type.name?lower_case}IndexedStructTable(final String name, final Class<S> structType, final TransformContext transformContext) {
        return new ${type.name}IndexedStructTableBuilder${classGenerics}(name, structType, transformContext);
    }

    public ${type.name}IndexedStructTableBuilder${classGenerics} replaceMetadata(final String fieldName, final Metadata metadata) {
        modifyMetadata(fieldName, metadata, this::replaceMetadataOnFd);
        return this;
    }

    public ${type.name}IndexedStructTableBuilder${classGenerics} updateMetadata(final String fieldName, final Metadata metadata) {
        modifyMetadata(fieldName, metadata, this::updateMetadataOnFd);
        return this;
    }

    public ${type.name}IndexedStructTableBuilder${classGenerics} fieldNamingStrategy(final FieldNamingStrategy fieldNamingStrategy) {
        this.fieldNamingStrategy = Objects.requireNonNullElse(fieldNamingStrategy, FieldNamingStrategy.Identity);
        return this;
    }

    private void modifyMetadata(
            final String fieldName,
            final Metadata metadata,
            final BiFunction<FieldDescriptor, Metadata, FieldDescriptor> modFunction) {
        if(fieldName.equals(keyField.name())) {
            keyField = modFunction.apply(keyField, metadata);
        }
        for (int i = 0, len = writableFields.size(); i < len; i++) {
            final FieldDescriptor fd = writableFields.get(i);
            if (fd.name().equals(fieldName)) {
                writableFields.set(i, modFunction.apply(fd, metadata));
                return;
            }
        }
        throw fieldNotFound("Field not found: " + fieldName);
    }

    private FieldDescriptor replaceMetadataOnFd(final FieldDescriptor fd, final Metadata metadata) {
        return new FieldDescriptor(fd.fieldType(), fd.name(), metadata);
    }

    private FieldDescriptor updateMetadataOnFd(final FieldDescriptor fd, final Metadata metadata) {
        return new FieldDescriptor(fd.fieldType(), fd.name(), fd.metadata().update(metadata));
    }

    private ${type.name}IndexedStructTable${classGenerics} internalBuild() {
        final var index = new ${type.name}IndexedSet${generics}(initialSize);
        final Map<Byte, List<FieldDescriptor>> typeMap = new HashMap<>(TypeId.Max + 1, 1);
        final StringGenericIndexedMap<SchemaField> fieldMap = new StringGenericIndexedMap<>(16);

        if(keyField != null) {
            keyField = renameIfChanged(keyField);
            final int fieldId = fieldMap.add(keyField.name());
            final Field field = IndexedSetFieldFactory.asKeyField(index);
            fieldMap.putValueAt(fieldId, SchemaField.schemaField(fieldId, keyField.name(), field, keyField.metadata()));
        }
        buildFieldCollections(fieldMap, typeMap);

        final TableStateChange change = new TableStateChange();
        final MatrixStoreFieldFactory fieldFactory =
                matrixStoreFieldFactory(initialSize, chunkSize, change.fieldChangeListener());
        final FieldList fieldList = fieldFactory.createFieldList(fieldMap, typeMap);
        final var facadeFactory = StructFacadeFactory.structFacadeFactory();
        return new ${type.name}IndexedStructTable<>(index, schema(name, fieldList), structType, change, facadeFactory);
    }

    private void buildFieldCollections(
            final StringGenericIndexedMap<SchemaField> fieldMap,
            final Map<Byte, List<FieldDescriptor>> typeMap) {
        writableFields.forEach(
                fd -> {
                    final var useFd = renameIfChanged(fd);
                    fieldMap.add(useFd.name());
                    typeMap.computeIfAbsent(useFd.fieldType(), ArrayList::new).add(useFd);
                });
    }

    private FieldDescriptor renameIfChanged(final FieldDescriptor fd) {
        final String newName = fieldNamingStrategy.formulateName(fd.name());
        return fd.name().equals(newName)
                ? fd
                : new FieldDescriptor(fd.fieldType(), newName, fd.metadata());
    }

}