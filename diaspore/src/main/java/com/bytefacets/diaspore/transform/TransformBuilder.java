// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.transform;

import static com.bytefacets.diaspore.common.Connector.connectOutputToInput;
import static com.bytefacets.diaspore.common.DefaultNameSupplier.resolveName;
import static com.bytefacets.diaspore.transform.DeferredTransformNode.deferredTransformNode;
import static com.bytefacets.diaspore.transform.ExplicitTransformNode.transformNode;

import com.bytefacets.diaspore.conflation.ChangeConflatorBuilder;
import com.bytefacets.diaspore.filter.FilterBuilder;
import com.bytefacets.diaspore.groupby.GroupByBuilder;
import com.bytefacets.diaspore.join.JoinBuilder;
import com.bytefacets.diaspore.printer.OutputLoggerBuilder;
import com.bytefacets.diaspore.projection.ProjectionBuilder;
import com.bytefacets.diaspore.prototype.PrototypeBuilder;
import com.bytefacets.diaspore.table.ByteIndexedStructTableBuilder;
import com.bytefacets.diaspore.table.ByteIndexedTableBuilder;
import com.bytefacets.diaspore.table.CharIndexedStructTableBuilder;
import com.bytefacets.diaspore.table.CharIndexedTableBuilder;
import com.bytefacets.diaspore.table.DoubleIndexedStructTableBuilder;
import com.bytefacets.diaspore.table.DoubleIndexedTableBuilder;
import com.bytefacets.diaspore.table.FloatIndexedStructTableBuilder;
import com.bytefacets.diaspore.table.FloatIndexedTableBuilder;
import com.bytefacets.diaspore.table.GenericIndexedStructTableBuilder;
import com.bytefacets.diaspore.table.GenericIndexedTableBuilder;
import com.bytefacets.diaspore.table.IntIndexedStructTableBuilder;
import com.bytefacets.diaspore.table.IntIndexedTableBuilder;
import com.bytefacets.diaspore.table.LongIndexedStructTableBuilder;
import com.bytefacets.diaspore.table.LongIndexedTableBuilder;
import com.bytefacets.diaspore.table.ShortIndexedStructTableBuilder;
import com.bytefacets.diaspore.table.ShortIndexedTableBuilder;
import com.bytefacets.diaspore.table.StringIndexedStructTableBuilder;
import com.bytefacets.diaspore.table.StringIndexedTableBuilder;
import com.bytefacets.diaspore.table.TableBuilder;
import com.bytefacets.diaspore.union.UnionBuilder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import javax.annotation.Nullable;

public final class TransformBuilder {
    private final Map<String, TransformNode<?>> nodeMap = new LinkedHashMap<>();
    private final List<TransformEdge> pendingEdges = new ArrayList<>();

    private TransformBuilder() {}

    public static TransformBuilder transform() {
        return new TransformBuilder();
    }

    public TableBuilder table(final String name) {
        return TableBuilder.table(name, this);
    }

    public ByteIndexedTableBuilder byteIndexedTable(final String name) {
        return ByteIndexedTableBuilder.byteIndexedTable(
                newContext(resolveName("ByteIndexedTable", name)));
    }

    public ShortIndexedTableBuilder shortIndexedTable(final String name) {
        return ShortIndexedTableBuilder.shortIndexedTable(
                newContext(resolveName("ShortIndexedTable", name)));
    }

    public CharIndexedTableBuilder charIndexedTable(final String name) {
        return CharIndexedTableBuilder.charIndexedTable(
                newContext(resolveName("CharIndexedTable", name)));
    }

    public IntIndexedTableBuilder intIndexedTable(final String name) {
        return IntIndexedTableBuilder.intIndexedTable(
                newContext(resolveName("IntIndexedTable", name)));
    }

    public LongIndexedTableBuilder longIndexedTable(final String name) {
        return LongIndexedTableBuilder.longIndexedTable(
                newContext(resolveName("LongIndexedTable", name)));
    }

    public FloatIndexedTableBuilder floatIndexedTable(final String name) {
        return FloatIndexedTableBuilder.floatIndexedTable(
                newContext(resolveName("FloatIndexedTable", name)));
    }

    public DoubleIndexedTableBuilder doubleIndexedTable(final String name) {
        return DoubleIndexedTableBuilder.doubleIndexedTable(
                newContext(resolveName("DoubleIndexedTable", name)));
    }

    public StringIndexedTableBuilder stringIndexedTable(final String name) {
        return StringIndexedTableBuilder.stringIndexedTable(
                newContext(resolveName("StringIndexedTable", name)));
    }

    public <K> GenericIndexedTableBuilder<K> genericIndexedTable(final String name) {
        return GenericIndexedTableBuilder.genericIndexedTable(
                newContext(resolveName("GenericIndexedTable", name)));
    }

    public <S> ByteIndexedStructTableBuilder<S> byteIndexedStructTable(final Class<S> type) {
        return ByteIndexedStructTableBuilder.byteIndexedStructTable(
                type, newContext(type.getSimpleName()));
    }

    public <S> ShortIndexedStructTableBuilder<S> shortIndexedStructTable(final Class<S> type) {
        return ShortIndexedStructTableBuilder.shortIndexedStructTable(
                type, newContext(type.getSimpleName()));
    }

    public <S> CharIndexedStructTableBuilder<S> charIndexedStructTable(final Class<S> type) {
        return CharIndexedStructTableBuilder.charIndexedStructTable(
                type, newContext(type.getSimpleName()));
    }

    public <S> IntIndexedStructTableBuilder<S> intIndexedStructTable(final Class<S> type) {
        return IntIndexedStructTableBuilder.intIndexedStructTable(
                type, newContext(type.getSimpleName()));
    }

    public <S> LongIndexedStructTableBuilder<S> longIndexedStructTable(final Class<S> type) {
        return LongIndexedStructTableBuilder.longIndexedStructTable(
                type, newContext(type.getSimpleName()));
    }

    public <S> FloatIndexedStructTableBuilder<S> floatIndexedStructTable(final Class<S> type) {
        return FloatIndexedStructTableBuilder.floatIndexedStructTable(
                type, newContext(type.getSimpleName()));
    }

    public <S> DoubleIndexedStructTableBuilder<S> doubleIndexedStructTable(final Class<S> type) {
        return DoubleIndexedStructTableBuilder.doubleIndexedStructTable(
                type, newContext(type.getSimpleName()));
    }

    public <S> StringIndexedStructTableBuilder<S> stringIndexedStructTable(final Class<S> type) {
        return StringIndexedStructTableBuilder.stringIndexedStructTable(
                type, newContext(type.getSimpleName()));
    }

    public <T, S> GenericIndexedStructTableBuilder<T, S> genericIndexedStructTable(
            final Class<S> type) {
        return GenericIndexedStructTableBuilder.genericIndexedStructTable(
                type, newContext(type.getSimpleName()));
    }

    public FilterBuilder filter() {
        return filter(null);
    }

    public FilterBuilder filter(final @Nullable String name) {
        return FilterBuilder.filter(newContext(resolveName("Filter", name)));
    }

    public GroupByBuilder groupBy() {
        return groupBy(null);
    }

    public GroupByBuilder groupBy(final @Nullable String name) {
        return GroupByBuilder.groupBy(newContext(resolveName("GroupBy", name)));
    }

    public ProjectionBuilder project() {
        return project(null);
    }

    public ProjectionBuilder project(final String name) {
        return ProjectionBuilder.projection(newContext(resolveName("Projection", name)));
    }

    public PrototypeBuilder prototyped(final String name) {
        return PrototypeBuilder.prototype(newContext(resolveName("Prototype", name)));
    }

    public PrototypeBuilder prototyped() {
        return prototyped(null);
    }

    public UnionBuilder union() {
        return union(null);
    }

    public UnionBuilder union(final String name) {
        return UnionBuilder.union(newContext(resolveName("Union", name)));
    }

    /**
     * @see com.bytefacets.diaspore.conflation.ChangeConflator
     */
    public ChangeConflatorBuilder changeConflator() {
        return changeConflator(null);
    }

    /**
     * @see com.bytefacets.diaspore.conflation.ChangeConflator
     */
    public ChangeConflatorBuilder changeConflator(final String name) {
        return ChangeConflatorBuilder.changeConflator(
                newContext(resolveName("ChangeConflator", name)));
    }

    /**
     * @see com.bytefacets.diaspore.join.Join
     */
    public JoinBuilder lookupJoin(final String name) {
        return JoinBuilder.lookupJoin(newContext(resolveName("Join", name)));
    }

    /**
     * @see com.bytefacets.diaspore.join.Join
     */
    public JoinBuilder lookupJoin() {
        return lookupJoin(null);
    }

    public OutputLoggerBuilder logger(final String name) {
        return OutputLoggerBuilder.logger(name);
    }

    public OutputLoggerBuilder logger() {
        return logger(null);
    }

    public TransformBuilder registerTransformNode(final TransformNode<?> node) {
        nodeMap.put(node.name(), node);
        return this;
    }

    public TransformBuilder registerNode(final String name, final Object operator) {
        nodeMap.put(name, transformNode(name, operator));
        return this;
    }

    public TransformBuilder registerDeferredNode(
            final String name, final Supplier<?> operatorSupplier) {
        nodeMap.put(name, deferredTransformNode(() -> name, operatorSupplier));
        return this;
    }

    public TransformContinuation createContinuation(
            final TransformNode<?> node, final OutputProvider outputProvider) {
        final var oldNode = nodeMap.get(node.name());
        if (oldNode != null && !Objects.equals(oldNode, node)) {
            throw TransformException.duplicate(node.name());
        }
        nodeMap.putIfAbsent(node.name(), node);
        return new TransformContinuation(this, node, outputProvider);
    }

    public TransformContinuation with(final String name, final OutputProvider outputProvider) {
        return createContinuation(transformNode(name, outputProvider), outputProvider);
    }

    public void registerEdge(
            final OutputProvider outputProvider, final InputProvider inputProvider) {
        pendingEdges.add(() -> connectOutputToInput(outputProvider, inputProvider));
    }

    public void registerEdgeWhenReady(
            final OutputProvider outputProvider, final InputProvider inputProvider) {
        pendingEdges.add(new TransformEdgeWhenReady(outputProvider, inputProvider));
    }

    public void build() {
        // touch the operators to resolve them if necessary
        // during this, it's possible that edges can be added, but expect no nodes
        nodeMap.values().forEach(TransformNode::operator);
        while (hasPending()) {
            final var edgeCopy = List.copyOf(pendingEdges);
            pendingEdges.clear();
            edgeCopy.forEach(TransformEdge::connect);
        }
    }

    private boolean hasPending() {
        return !pendingEdges.isEmpty();
    }

    @SuppressWarnings("unchecked")
    public <T> T lookupNode(final String name) {
        return (T) lookupOperatorInternal(name);
    }

    Object lookupOperatorInternal(final String name) {
        final TransformNode<?> node = nodeMap.get(name);
        if (node != null) {
            return node.operator();
        }
        throw TransformException.notFound(name);
    }

    OutputProvider lookupOutputProvider(final String name) {
        final Object operator = lookupOperatorInternal(name);
        if (operator instanceof OutputProvider outputProvider) {
            return outputProvider;
        } else {
            throw TransformException.notAnOutputProvider(name, operator);
        }
    }

    private TransformContext newContext(final String name) {
        return new TransformContext(name, this);
    }
}
