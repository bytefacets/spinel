// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.prototype;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public final class PrototypeBuilder {
    private final BuilderSupport<Prototype> builderSupport;
    private final Map<String, FieldDescriptor> fields = new HashMap<>();
    private final String name;
    private final TransformContext transformContext;
    private int deletedRowBatchSize = 64;

    private PrototypeBuilder(final String name) {
        this.name = requireNonNull(name, "name");
        this.builderSupport = builderSupport(name, this::internalBuild);
        this.transformContext = null;
    }

    private PrototypeBuilder(final TransformContext context) {
        this.name = context.name();
        this.transformContext = requireNonNull(context, "transform context");
        this.builderSupport =
                context.createBuilderSupport(this::internalBuild, () -> getOrCreate().input());
    }

    public static PrototypeBuilder prototype() {
        return prototype((String) null);
    }

    public static PrototypeBuilder prototype(final String name) {
        return new PrototypeBuilder(resolveName("Prototype", name));
    }

    public static PrototypeBuilder prototype(final TransformContext transform) {
        return new PrototypeBuilder(requireNonNull(transform, "transform context"));
    }

    public PrototypeBuilder deleteRowBatchSize(final int deletedRowBatchSize) {
        this.deletedRowBatchSize = deletedRowBatchSize;
        return this;
    }

    public PrototypeBuilder addFields(final FieldDescriptor... fieldDescriptors) {
        Stream.of(fieldDescriptors).forEach(this::addField);
        return this;
    }

    public PrototypeBuilder addField(final FieldDescriptor fieldDescriptor) {
        if (fields.containsKey(fieldDescriptor.name())) {
            throw setupException(
                    String.format(
                            "Duplicate field name setting up prototype (%s): %s",
                            requireNonNullElse(name, "unnamed"), fieldDescriptor.name()));
        }
        fields.put(fieldDescriptor.name(), fieldDescriptor);
        return this;
    }

    public Prototype getOrCreate() {
        return builderSupport.getOrCreate();
    }

    public Prototype build() {
        return builderSupport.createOperator();
    }

    public TransformContinuation then() {
        return continuation(
                transformContext, builderSupport.transformNode(), () -> getOrCreate().output());
    }

    private Prototype internalBuild() {
        builderSupport.throwIfBuilt();
        if (fields.isEmpty()) {
            throw setupException("Prototype must have fields declared, but this one has none");
        }
        return new Prototype(
                new PrototypeSchemaBuilder(name, List.copyOf(fields.values())),
                deletedRowBatchSize);
    }
}
