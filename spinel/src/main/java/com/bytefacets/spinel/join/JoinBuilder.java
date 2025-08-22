// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.join;

import static com.bytefacets.spinel.common.DefaultNameSupplier.resolveName;
import static com.bytefacets.spinel.join.DynamicJoinInterner.dynamicJoinInterner;
import static com.bytefacets.spinel.transform.BuilderSupport.builderSupport;
import static com.bytefacets.spinel.transform.TransformContext.continuation;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElseGet;

import com.bytefacets.spinel.common.NameConflictResolver;
import com.bytefacets.spinel.transform.BuilderSupport;
import com.bytefacets.spinel.transform.TransformContext;
import com.bytefacets.spinel.transform.TransformContinuation;
import java.util.List;
import jakarta.annotation.Nullable;

public final class JoinBuilder {
    private final String name;
    private final TransformContext context;
    private final BuilderSupport<Join> builderSupport;
    private final JoinType type;
    private boolean outer;
    private int initialLeftCapacity = 128;
    private int initialRightCapacity = 128;
    private String leftSourceRowFieldName;
    private String rightSourceRowFieldName;
    private JoinInterner joinInterner;
    private JoinKeyHandling joinKeyHandling = JoinKeyHandling.KeepAll;
    private String leftSourceNodeName;
    private String rightSourceNodeName;

    private JoinBuilder(final String name, final JoinType type) {
        this.name = name;
        this.context = null;
        this.type = requireNonNull(type, "type");
        this.builderSupport = builderSupport(name, this::internalBuild);
    }

    private JoinBuilder(final TransformContext context, final JoinType type) {
        this.name = context.name();
        this.context = context;
        this.type = requireNonNull(type, "type");
        this.builderSupport = context.createBuilderSupport(this::internalBuild, null);
    }

    public Join build() {
        return builderSupport.createOperator();
    }

    public Join getOrCreate() {
        return builderSupport.getOrCreate();
    }

    public TransformContinuation then() {
        return continuation(context, builderSupport.transformNode(), () -> getOrCreate().output());
    }

    private Join internalBuild() {
        final JoinChangeTracker tracker = JoinChangeTracker.stateChangeSet();
        final JoinMapper mapper = selectMapper(joinInterner, tracker);
        final NameConflictResolver nameResolver = new NameConflictResolver() {};
        final var join =
                new Join(
                        new JoinSchemaBuilder(
                                name,
                                leftSourceRowFieldName,
                                rightSourceRowFieldName,
                                mapper,
                                joinInterner,
                                nameResolver,
                                joinKeyHandling),
                        tracker,
                        mapper);
        if (leftSourceNodeName != null) {
            context.registerEdge(leftSourceNodeName, join::leftInput);
        }
        if (rightSourceNodeName != null) {
            context.registerEdge(rightSourceNodeName, join::rightInput);
        }
        return join;
    }

    private JoinMapper selectMapper(final JoinInterner interner, final JoinChangeTracker tracker) {
        return new LookupJoinMapper(
                interner, tracker, initialLeftCapacity, initialRightCapacity, outer);
    }

    public JoinBuilder joinOn(final JoinInterner joinInterner) {
        this.joinInterner = joinInterner;
        return this;
    }

    public JoinBuilder withLeftSource(final String nodeName) {
        requireContext(String.format("withLeftFrom(\"%s\")", nodeName));
        leftSourceNodeName = requireNonNull(nodeName, "nodeName");
        return this;
    }

    public JoinBuilder withRightSource(final String nodeName) {
        requireContext(String.format("withRightFrom(\"%s\")", nodeName));
        rightSourceNodeName = requireNonNull(nodeName, "nodeName");
        return this;
    }

    public JoinBuilder joinOn(
            final List<String> leftFieldNames,
            final List<String> rightFieldNames,
            final int initialJoinCapacity) {
        final List<String> leftJoinFields = requireNonNullElseGet(leftFieldNames, List::of);
        final List<String> rightJoinFields = requireNonNullElseGet(rightFieldNames, List::of);
        if (leftJoinFields.size() != rightJoinFields.size()) {
            throw new IllegalArgumentException(
                    "Left and Right fields names for joins should have the same number: left="
                            + leftJoinFields
                            + ", right="
                            + rightJoinFields);
        }
        this.joinInterner =
                dynamicJoinInterner(leftJoinFields, rightJoinFields, initialJoinCapacity);
        return this;
    }

    public JoinBuilder includeLeftSourceRowAs(final String leftSourceRowFieldName) {
        this.leftSourceRowFieldName = leftSourceRowFieldName;
        return this;
    }

    public JoinBuilder includeRightSourceRowAs(final String rightSourceRowFieldName) {
        this.rightSourceRowFieldName = rightSourceRowFieldName;
        return this;
    }

    public JoinBuilder withInitialLeftCapacity(final int initialLeftCapacity) {
        this.initialLeftCapacity = initialLeftCapacity;
        return this;
    }

    public JoinBuilder withInitialRightCapacity(final int initialRightCapacity) {
        this.initialRightCapacity = initialRightCapacity;
        return this;
    }

    public JoinBuilder withJoinKeyHandling(final JoinKeyHandling handling) {
        this.joinKeyHandling = requireNonNull(handling, "handling");
        return this;
    }

    public JoinBuilder outer() {
        this.outer = true;
        return this;
    }

    public JoinBuilder inner() {
        this.outer = false;
        return this;
    }

    public static JoinBuilder lookupJoin() {
        return lookupJoin((String) null);
    }

    public static JoinBuilder lookupJoin(final @Nullable String name) {
        return new JoinBuilder(resolveName("Join", name), JoinType.Lookup);
    }

    public static JoinBuilder lookupJoin(final TransformContext context) {
        return new JoinBuilder(context, JoinType.Lookup);
    }

    private void requireContext(final String name) {
        if (context == null) {
            throw new RuntimeException(
                    String.format(
                            "Can only use %s when in the context of a TransformBuilder", name));
        }
    }

    enum JoinType {
        Lookup
    }
}
