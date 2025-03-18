// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.transform;

import static com.bytefacets.diaspore.common.DefaultNameSupplier.resolveName;

import com.bytefacets.diaspore.conflation.ChangeConflatorBuilder;
import com.bytefacets.diaspore.filter.FilterBuilder;
import com.bytefacets.diaspore.groupby.GroupByBuilder;
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

    public <S> ByteIndexedStructTableBuilder<S> byteIndexedStructTable(final Class<S> type) {
        return ByteIndexedStructTableBuilder.byteIndexedStructTable(type);
    }

    public <S> ShortIndexedStructTableBuilder<S> shortIndexedStructTable(final Class<S> type) {
        return ShortIndexedStructTableBuilder.shortIndexedStructTable(type);
    }

    public <S> CharIndexedStructTableBuilder<S> charIndexedStructTable(final Class<S> type) {
        return CharIndexedStructTableBuilder.charIndexedStructTable(type);
    }

    public <S> IntIndexedStructTableBuilder<S> intIndexedStructTable(final Class<S> type) {
        return IntIndexedStructTableBuilder.intIndexedStructTable(type);
    }

    public <S> LongIndexedStructTableBuilder<S> longIndexedStructTable(final Class<S> type) {
        return LongIndexedStructTableBuilder.longIndexedStructTable(type);
    }

    public <S> FloatIndexedStructTableBuilder<S> floatIndexedStructTable(final Class<S> type) {
        return FloatIndexedStructTableBuilder.floatIndexedStructTable(type);
    }

    public <S> DoubleIndexedStructTableBuilder<S> doubleIndexedStructTable(final Class<S> type) {
        return DoubleIndexedStructTableBuilder.doubleIndexedStructTable(type);
    }

    public <S> StringIndexedStructTableBuilder<S> stringIndexedStructTable(final Class<S> type) {
        return StringIndexedStructTableBuilder.stringIndexedStructTable(type);
    }

    public <T, S> GenericIndexedStructTableBuilder<T, S> genericIndexedStructTable(
            final Class<S> type) {
        return GenericIndexedStructTableBuilder.genericIndexedStructTable(type);
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
