// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.table;

import static com.bytefacets.spinel.common.DefaultNameSupplier.resolveName;
import static com.bytefacets.spinel.exception.OperatorSetupException.setupException;
import static com.bytefacets.spinel.schema.MatrixStoreFieldFactory.matrixStoreFieldFactory;
import static com.bytefacets.spinel.schema.Schema.schema;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.hash.StringGenericIndexedMap;
import com.bytefacets.spinel.schema.FieldDescriptor;
import com.bytefacets.spinel.schema.FieldList;
import com.bytefacets.spinel.schema.MatrixStoreFieldFactory;
import com.bytefacets.spinel.schema.SchemaField;
import com.bytefacets.spinel.schema.TypeId;
import com.bytefacets.spinel.transform.BuilderSupport;
import com.bytefacets.spinel.transform.TransformBuilder;
import com.bytefacets.spinel.transform.TransformContinuation;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public final class TableBuilder {
    private final Set<String> names = new HashSet<>();
    private final Map<Byte, List<FieldDescriptor>> typeMap = new HashMap<>(TypeId.Max + 1, 1);
    private final StringGenericIndexedMap<SchemaField> fieldMap = new StringGenericIndexedMap<>(16);
    private final TransformBuilder transform;
    private final BuilderSupport<Table> builderSupport;
    private final String name;
    private int initialSize = 64;
    private int chunkSize = 64;

    private TableBuilder(final @Nullable String name, final @Nullable TransformBuilder transform) {
        this.name = resolveName("Table", name);
        this.builderSupport = BuilderSupport.builderSupport(this.name, this::internalBuild);
        this.transform = transform;
        if (transform != null) {
            transform.registerTransformNode(builderSupport.transformNode());
        }
    }

    public String name() {
        return name;
    }

    public TableBuilder initialSize(final int initialSize) {
        this.initialSize = initialSize;
        return this;
    }

    public TableBuilder chunkSize(final int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    public static TableBuilder table() {
        return new TableBuilder(null, null);
    }

    public static TableBuilder table(final String name) {
        return new TableBuilder(name, null);
    }

    public static TableBuilder table(final String name, final TransformBuilder transform) {
        return new TableBuilder(name, requireNonNull(transform, "transform"));
    }

    public TransformContinuation then() {
        if (transform == null) {
            throw setupException(
                    "'then' must be called in the context of building a Transform using TransformBuilder");
        }
        return transform.createContinuation(
                builderSupport.transformNode(), () -> getOrCreate().output());
    }

    public TableBuilder addField(final FieldDescriptor fieldDescriptor) {
        if (names.add(fieldDescriptor.name())) {
            fieldMap.add(fieldDescriptor.name()); // fields appear in order
            typeMap.computeIfAbsent(fieldDescriptor.fieldType(), ArrayList::new)
                    .add(fieldDescriptor);
        }
        return this;
    }

    public TableBuilder addFields(final FieldDescriptor... fieldDescriptor) {
        Stream.of(fieldDescriptor).forEach(this::addField);
        return this;
    }

    public Table getOrCreate() {
        return builderSupport.getOrCreate();
    }

    private Table internalBuild() {
        builderSupport.throwIfBuilt();
        final TableStateChange change = new TableStateChange();
        final MatrixStoreFieldFactory fieldFactory =
                matrixStoreFieldFactory(initialSize, chunkSize, change.fieldChangeListener());
        final FieldList fields = fieldFactory.createFieldList(fieldMap, typeMap);
        return new Table(schema(name, fields), change);
    }

    public Table build() {
        return builderSupport.createOperator();
    }
}
