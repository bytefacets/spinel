// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.transform;

import static com.bytefacets.diaspore.common.DefaultNameSupplier.resolveName;

import com.bytefacets.diaspore.filter.FilterBuilder;
import com.bytefacets.diaspore.groupby.GroupByBuilder;
import com.bytefacets.diaspore.projection.ProjectionBuilder;
import com.bytefacets.diaspore.prototype.PrototypeBuilder;
import com.bytefacets.diaspore.table.ByteIndexedTableBuilder;
import com.bytefacets.diaspore.table.CharIndexedTableBuilder;
import com.bytefacets.diaspore.table.DoubleIndexedTableBuilder;
import com.bytefacets.diaspore.table.FloatIndexedTableBuilder;
import com.bytefacets.diaspore.table.GenericIndexedTableBuilder;
import com.bytefacets.diaspore.table.IntIndexedTableBuilder;
import com.bytefacets.diaspore.table.LongIndexedTableBuilder;
import com.bytefacets.diaspore.table.ShortIndexedTableBuilder;
import com.bytefacets.diaspore.table.StringIndexedTableBuilder;
import com.bytefacets.diaspore.table.TableBuilder;
import com.bytefacets.diaspore.union.UnionBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

public final class TransformBuilder {
    private final List<TransformNode<?>> nodes = new ArrayList<>();
    private final List<TransformEdge> edges = new ArrayList<>();
    private final Map<String, Object> namedNodeMap = new HashMap<>();

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

    public void registerTransformNode(final TransformNode<?> node) {
        nodes.add(node);
    }

    public TransformContinuation createContinuation(
            final TransformNode<?> node, final OutputProvider outputProvider) {
        return new TransformContinuation(this, node, outputProvider);
    }

    public void registerEdge(
            final OutputProvider outputProvider, final InputProvider inputProvider) {
        edges.add(() -> outputProvider.output().attachInput(inputProvider.input()));
    }

    public void registerEdgeWhenReady(
            final OutputProvider outputProvider, final InputProvider inputProvider) {
        edges.add(new TransformEdgeWhenReady(outputProvider, inputProvider));
    }

    public void build() {
        edges.forEach(TransformEdge::connect);
        nodes.forEach(node -> namedNodeMap.put(node.name(), node.operator()));
        // maybe separate this into pending and done so we can do build() multiple times
        edges.clear();
        nodes.clear();
    }

    @SuppressWarnings("unchecked")
    public <T> T lookupNode(final String name) {
        return (T) namedNodeMap.get(name);
    }

    private TransformContext newContext(final String name) {
        return new TransformContext(name, this);
    }
}
