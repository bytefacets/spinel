// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.vaadin.grid;

import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.comms.send.SubscriptionContainer;
import com.bytefacets.spinel.ui.Pager;
import io.netty.channel.EventLoop;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Builds a {@link GridAdapter}. */
public final class GridAdapterBuilder {
    private final SubscriptionContainer subscription;
    private final EventLoop eventLoop;
    private final List<String> fieldOrder = new ArrayList<>(4);
    private final Map<String, ColumnSetup> columnCustomization = new HashMap<>(4);
    private int initialPagerSize = 128;
    private int pagerChunkSize = 128;

    private GridAdapterBuilder(
            final SubscriptionContainer subscription, final EventLoop eventLoop) {
        this.subscription = requireNonNull(subscription, "subscription");
        this.eventLoop = requireNonNull(eventLoop, "eventLoop");
    }

    public static GridAdapterBuilder gridAdapter(
            final SubscriptionContainer subscription, final EventLoop eventLoop) {
        return new GridAdapterBuilder(subscription, eventLoop);
    }

    public GridAdapterBuilder customizeColumn(final String columnName, final ColumnSetup setup) {
        columnCustomization.put(columnName, setup);
        return this;
    }

    public GridAdapterBuilder fieldOrder(final String... fields) {
        this.fieldOrder.clear();
        this.fieldOrder.addAll(Arrays.asList(fields));
        return this;
    }

    public GridAdapterBuilder fieldOrder(final List<String> fields) {
        this.fieldOrder.clear();
        this.fieldOrder.addAll(fields);
        return this;
    }

    public GridAdapterBuilder initialPagerSize(final int initialPagerSize) {
        this.initialPagerSize = initialPagerSize;
        return this;
    }

    public GridAdapterBuilder pagerChunkSize(final int pagerChunkSize) {
        this.pagerChunkSize = pagerChunkSize;
        return this;
    }

    public GridAdapter build() {
        return new GridAdapter(
                subscription,
                Pager.pager(initialPagerSize, pagerChunkSize),
                fieldOrder,
                columnCustomization,
                eventLoop);
    }
}
