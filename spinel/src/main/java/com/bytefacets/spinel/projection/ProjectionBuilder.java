// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.projection;

import static com.bytefacets.spinel.common.DefaultNameSupplier.resolveName;
import static com.bytefacets.spinel.exception.OperatorSetupException.setupException;
import static com.bytefacets.spinel.transform.BuilderSupport.builderSupport;
import static com.bytefacets.spinel.transform.TransformContext.continuation;
import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.schema.Metadata;
import com.bytefacets.spinel.schema.SchemaField;
import com.bytefacets.spinel.transform.BuilderSupport;
import com.bytefacets.spinel.transform.TransformContext;
import com.bytefacets.spinel.transform.TransformContinuation;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import jakarta.annotation.Nullable;

public final class ProjectionBuilder {
    private final BuilderSupport<Projection> builderSupport;
    private final TransformContext transformContext;
    private String[] orderOnLeft;
    private String[] orderOnRight;
    private Set<String> omittedFields;
    private Set<String> includedFields;
    private final Map<String, String> inboundAliases = new HashMap<>(4);
    private final Map<String, CalculatedFieldDescriptor> lazyCalcs = new LinkedHashMap<>(4);
    private final String name;

    private ProjectionBuilder(final String name) {
        this.name = requireNonNull(name);
        this.builderSupport = builderSupport(name, this::internalBuild);
        this.transformContext = null;
    }

    private ProjectionBuilder(final TransformContext context) {
        this.transformContext = requireNonNull(context, "transform context");
        this.name = context.name();
        this.builderSupport =
                context.createBuilderSupport(this::internalBuild, () -> getOrCreate().input());
    }

    public static ProjectionBuilder projection() {
        return projection((String) null);
    }

    public static ProjectionBuilder projection(final @Nullable String name) {
        return new ProjectionBuilder(resolveName("Projection", name));
    }

    public static ProjectionBuilder projection(final TransformContext transformContext) {
        return new ProjectionBuilder(transformContext);
    }

    public ProjectionBuilder inboundAlias(final String inboundName, final String outboundName) {
        inboundAliases.put(inboundName, outboundName);
        return this;
    }

    public Projection getOrCreate() {
        return builderSupport.getOrCreate();
    }

    public Projection build() {
        return builderSupport.createOperator();
    }

    public TransformContinuation then() {
        return continuation(
                transformContext, builderSupport.transformNode(), () -> getOrCreate().output());
    }

    private Projection internalBuild() {
        return new Projection(schemaBuilder());
    }

    ProjectionSchemaBuilder schemaBuilder() {
        return new ProjectionSchemaBuilder(
                name,
                fieldSelector(),
                inboundAliases,
                lazyCalcs,
                new FieldSorter(orderOnLeft, orderOnRight));
    }

    public ProjectionBuilder outboundOrderOnLeft(final String... orderOnLeft) {
        this.orderOnLeft = orderOnLeft;
        return this;
    }

    public ProjectionBuilder outboundOrderOnRight(final String... orderOnRight) {
        this.orderOnRight = orderOnRight;
        return this;
    }

    public ProjectionBuilder omit(final String... omitted) {
        if (omittedFields == null) {
            omittedFields = new HashSet<>(omitted.length);
        }
        omittedFields.addAll(List.of(omitted));
        return this;
    }

    public ProjectionBuilder include(final String... included) {
        if (includedFields == null) {
            includedFields = new HashSet<>(included.length);
        }
        includedFields.addAll(List.of(included));
        return this;
    }

    public ProjectionBuilder lazyCalculation(final String name, final FieldCalculation calc) {
        return lazyCalculation(name, calc, Metadata.EMPTY);
    }

    public ProjectionBuilder lazyCalculation(
            final String name, final FieldCalculation calc, final Metadata metadata) {
        this.lazyCalcs.put(name, new CalculatedFieldDescriptor(name, calc, metadata));
        return this;
    }

    private InboundFieldSelector fieldSelector() {
        final Set<String> included = includedFields != null ? new HashSet<>(includedFields) : null;
        final Set<String> omitted = omittedFields != null ? Set.copyOf(omittedFields) : null;
        return (projectionName, fieldList) -> {
            final Map<String, SchemaField> result = new HashMap<>(fieldList.size());
            for (int i = 0, len = fieldList.size(); i < len; i++) {
                final var field = fieldList.fieldAt(i);
                final boolean include = included == null || included.remove(field.name());
                final boolean omit = omitted != null && omitted.contains(field.name());
                if (include && !omit) {
                    result.put(field.name(), field);
                }
            }
            if (included != null && !included.isEmpty()) {
                throw setupException(
                        String.format(
                                "Exception setting up projection %s; "
                                        + "included fields not found in inbound schema: %s",
                                projectionName, included));
            }
            return result;
        };
    }
}
