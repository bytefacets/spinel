// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.table;

import static com.bytefacets.spinel.exception.FieldNotFoundException.fieldNotFound;
import static com.bytefacets.spinel.exception.OperatorSetupException.setupException;
import static com.bytefacets.spinel.facade.StructFacadeFactory.structFacadeFactory;
import static com.bytefacets.spinel.schema.MatrixStoreFieldFactory.matrixStoreFieldFactory;
import static com.bytefacets.spinel.schema.Schema.schema;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

import com.bytefacets.collections.hash.StringGenericIndexedMap;
import com.bytefacets.spinel.facade.FieldNamingStrategy;
import com.bytefacets.spinel.facade.StructFieldExtractor;
import com.bytefacets.spinel.schema.FieldDescriptor;
import com.bytefacets.spinel.schema.FieldList;
import com.bytefacets.spinel.schema.MatrixStoreFieldFactory;
import com.bytefacets.spinel.schema.Metadata;
import com.bytefacets.spinel.schema.SchemaField;
import com.bytefacets.spinel.schema.TypeId;
import com.bytefacets.spinel.transform.BuilderSupport;
import com.bytefacets.spinel.transform.TransformBuilder;
import com.bytefacets.spinel.transform.TransformContinuation;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public final class StructTableBuilder<T> {
    private final Class<T> type;
    private final List<FieldDescriptor> fieldDescriptors = new ArrayList<>(8);
    private final TransformBuilder transform;
    private final BuilderSupport<StructTable<T>> builderSupport;
    private final String name;
    private FieldNamingStrategy fieldNamingStrategy = FieldNamingStrategy.Identity;
    private int initialSize = 64;
    private int chunkSize = 64;

    private StructTableBuilder(
            final String name, final Class<T> type, final @Nullable TransformBuilder transform) {
        this.type = requireNonNull(type, "type");
        this.name = requireNonNull(name, "name");
        // collect the writable fields
        StructFieldExtractor.consumeFields(type, fieldDescriptors::add, fd -> {});
        this.builderSupport = BuilderSupport.builderSupport(this.name, this::internalBuild);
        this.transform = transform;
        if (transform != null) {
            transform.registerTransformNode(builderSupport.transformNode());
        }
    }

    public StructTableBuilder<T> replaceMetadata(final String fieldName, final Metadata metadata) {
        modifyMetadata(fieldName, metadata, this::replaceMetadataOnFd);
        return this;
    }

    public StructTableBuilder<T> updateMetadata(final String fieldName, final Metadata metadata) {
        modifyMetadata(fieldName, metadata, this::updateMetadataOnFd);
        return this;
    }

    public StructTableBuilder<T> fieldNamingStrategy(
            final FieldNamingStrategy fieldNamingStrategy) {
        this.fieldNamingStrategy =
                requireNonNullElse(fieldNamingStrategy, FieldNamingStrategy.Identity);
        return this;
    }

    private void modifyMetadata(
            final String fieldName,
            final Metadata metadata,
            final BiFunction<FieldDescriptor, Metadata, FieldDescriptor> modFunction) {
        for (int i = 0, len = fieldDescriptors.size(); i < len; i++) {
            final FieldDescriptor fd = fieldDescriptors.get(i);
            if (fd.name().equals(fieldName)) {
                fieldDescriptors.set(i, modFunction.apply(fd, metadata));
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

    public String name() {
        return name;
    }

    public StructTableBuilder<T> initialSize(final int initialSize) {
        this.initialSize = initialSize;
        return this;
    }

    public StructTableBuilder<T> chunkSize(final int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    public static <T> StructTableBuilder<T> table(final Class<T> type) {
        return new StructTableBuilder<>(type.getSimpleName(), type, null);
    }

    public static <T> StructTableBuilder<T> table(final String name, final Class<T> type) {
        return new StructTableBuilder<>(name, type, null);
    }

    public static <T> StructTableBuilder<T> table(
            final Class<T> type, final TransformBuilder transform) {
        return new StructTableBuilder<>(
                type.getSimpleName(), type, requireNonNull(transform, "transform"));
    }

    public static <T> StructTableBuilder<T> table(
            final String name, final Class<T> type, final TransformBuilder transform) {
        return new StructTableBuilder<>(name, type, requireNonNull(transform, "transform"));
    }

    public TransformContinuation then() {
        if (transform == null) {
            throw setupException(
                    "'then' must be called in the context of building a Transform using TransformBuilder");
        }
        return transform.createContinuation(
                builderSupport.transformNode(), () -> getOrCreate().output());
    }

    public StructTable<T> getOrCreate() {
        return builderSupport.getOrCreate();
    }

    private StructTable<T> internalBuild() {
        builderSupport.throwIfBuilt();
        final Map<Byte, List<FieldDescriptor>> typeMap = new HashMap<>(TypeId.Max + 1, 1);
        final StringGenericIndexedMap<SchemaField> fieldMap = new StringGenericIndexedMap<>(16);
        buildFieldCollections(fieldMap, typeMap);
        final TableStateChange change = new TableStateChange();
        final MatrixStoreFieldFactory fieldFactory =
                matrixStoreFieldFactory(initialSize, chunkSize, change.fieldChangeListener());
        final FieldList fields = fieldFactory.createFieldList(fieldMap, typeMap);
        return new StructTable<>(schema(name, fields), type, change, structFacadeFactory());
    }

    private void buildFieldCollections(
            final StringGenericIndexedMap<SchemaField> fieldMap,
            final Map<Byte, List<FieldDescriptor>> typeMap) {
        fieldDescriptors.forEach(
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

    public StructTable<T> build() {
        return builderSupport.createOperator();
    }
}
