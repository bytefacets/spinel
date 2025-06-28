// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.table;

import static com.bytefacets.diaspore.exception.OperatorSetupException.setupException;
import static com.bytefacets.diaspore.facade.StructFacadeFactory.structFacadeFactory;
import static com.bytefacets.diaspore.schema.MatrixStoreFieldFactory.matrixStoreFieldFactory;
import static com.bytefacets.diaspore.schema.Schema.schema;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.hash.StringGenericIndexedMap;
import com.bytefacets.diaspore.facade.StructFieldExtractor;
import com.bytefacets.diaspore.schema.FieldDescriptor;
import com.bytefacets.diaspore.schema.FieldList;
import com.bytefacets.diaspore.schema.MatrixStoreFieldFactory;
import com.bytefacets.diaspore.schema.SchemaField;
import com.bytefacets.diaspore.schema.TypeId;
import com.bytefacets.diaspore.transform.BuilderSupport;
import com.bytefacets.diaspore.transform.TransformBuilder;
import com.bytefacets.diaspore.transform.TransformContinuation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

public final class StructTableBuilder<T> {
    private final Class<T> type;
    private final Map<Byte, List<FieldDescriptor>> typeMap = new HashMap<>(TypeId.Max + 1, 1);
    private final StringGenericIndexedMap<SchemaField> fieldMap = new StringGenericIndexedMap<>(16);
    private final TransformBuilder transform;
    private final BuilderSupport<StructTable<T>> builderSupport;
    private final String name;
    private int initialSize = 64;
    private int chunkSize = 64;

    private StructTableBuilder(final Class<T> type, final @Nullable TransformBuilder transform) {
        this.type = requireNonNull(type, "type");
        this.name = type.getSimpleName();
        StructFieldExtractor.getFieldList(type)
                .forEach(
                        fd -> {
                            fieldMap.add(fd.name());
                            typeMap.computeIfAbsent(fd.fieldType(), ArrayList::new).add(fd);
                        });
        this.builderSupport = BuilderSupport.builderSupport(this.name, this::internalBuild);
        this.transform = transform;
        if (transform != null) {
            transform.registerTransformNode(builderSupport.transformNode());
        }
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
        return new StructTableBuilder<>(type, null);
    }

    public static <T> StructTableBuilder<T> table(
            final Class<T> type, final TransformBuilder transform) {
        return new StructTableBuilder<>(type, requireNonNull(transform, "transform"));
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
        final TableStateChange change = new TableStateChange();
        final MatrixStoreFieldFactory fieldFactory =
                matrixStoreFieldFactory(initialSize, chunkSize, change.fieldChangeListener());
        final FieldList fields = fieldFactory.createFieldList(fieldMap, typeMap);
        return new StructTable<>(schema(name, fields), type, change, structFacadeFactory());
    }

    public StructTable<T> build() {
        return builderSupport.createOperator();
    }
}
