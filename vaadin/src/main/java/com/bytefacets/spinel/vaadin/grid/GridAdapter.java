// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.vaadin.grid;

import static com.bytefacets.spinel.vaadin.data.FieldValueProvider.valueProvider;
import static com.bytefacets.spinel.vaadin.data.RendererFactory.textRenderer;
import static com.bytefacets.spinel.vaadin.data.TransformDataProvider.transformDataProvider;
import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.comms.send.SubscriptionContainer;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.SchemaField;
import com.bytefacets.spinel.schema.TypeId;
import com.bytefacets.spinel.ui.Pager;
import com.bytefacets.spinel.vaadin.data.FieldValueProvider;
import com.bytefacets.spinel.vaadin.data.TransformDataProvider;
import com.bytefacets.spinel.vaadin.data.TransformRow;
import com.vaadin.flow.component.UI;
import com.vaadin.flow.component.grid.ColumnTextAlign;
import com.vaadin.flow.component.grid.Grid;
import io.netty.channel.EventLoop;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An adapter that creates a Grid component from a Spinel Subscription. This means that the columns
 * are dynamically added from the subscription's output and text rendering is derived from the
 * schema field's metadata. The GridAdapterBuilder has some knobs for sizing of some internal
 * components, field ordering, and for additional column customization (such as different
 * renderers).
 *
 * <p>Example
 *
 * <pre>
 *   var provider = defaultSubscriptionProvider(outputRegistry);
 *   var userSession = new ConnectedUserSession(authenticationContext);
 *   var subscriptionConfig =
 *           SubscriptionConfig.subscriptionConfig("order-view").defaultAll(true).build();
 *   SubscriptionContainer sub =
 *           provider.getSubscription(userSession, subscriptionConfig, List.of());
 *   this.gridAdapter = GridAdapterBuilder.gridAdapter(sub, eventLoop).build();
 *   gridAdapter.refreshInterval(UI.getCurrent(), Duration.ofMillis(250));
 *   addDetachListener(this::disconnect);
 * </pre>
 */
public final class GridAdapter {
    private final Grid<TransformRow> grid;
    private final TransformDataProvider dataProvider;
    private final SubscriptionContainer subscription;
    private final EventLoop eventLoop;
    private final List<String> fieldOrder;
    private final Map<String, ColumnSetup> columnCustomization;

    GridAdapter(
            final SubscriptionContainer subscription,
            final Pager pager,
            final List<String> fieldOrder,
            final Map<String, ColumnSetup> columnCustomization,
            final EventLoop eventLoop) {
        this.grid = new Grid<>(TransformRow.class, false);
        this.subscription = requireNonNull(subscription, "subscription");
        this.eventLoop = requireNonNull(eventLoop, "eventLoop");
        this.fieldOrder = List.copyOf(requireNonNull(fieldOrder, "fieldOrder"));
        this.columnCustomization =
                Map.copyOf(requireNonNull(columnCustomization, "columnCustomization"));
        this.dataProvider =
                transformDataProvider(subscription, pager, this::onSchemaOnUiThread, eventLoop);
        grid.setDataProvider(dataProvider);
    }

    public Grid<TransformRow> grid() {
        return grid;
    }

    public void refreshInterval(final UI ui, final Duration interval) {
        ui.setPollInterval((int) interval.toMillis());
        ui.addPollListener(e -> dataProvider.refreshAll());
    }

    public void disconnect() {
        eventLoop.execute(subscription::terminateSubscription);
    }

    private void onSchemaOnUiThread(final Schema schema) {
        if (schema != null) {
            grid.removeAllColumns();
            if (fieldOrder.isEmpty()) {
                schema.forEachField(this::addField);
            } else {
                // NOTE this is lenient - allows messing up field names
                fieldOrder.stream()
                        .map(schema::maybeField)
                        .filter(Objects::nonNull)
                        .forEach(this::addField);
            }
        }
    }

    private void addField(final SchemaField schemaField) {
        final FieldValueProvider valueProvider = valueProvider(schemaField.field());
        final Grid.Column<TransformRow> col = grid.addColumn(valueProvider);
        col.setHeader(schemaField.name());
        col.setTextAlign(
                TypeId.toClass(schemaField.typeId()).isAssignableFrom(Number.class)
                        ? ColumnTextAlign.END
                        : ColumnTextAlign.START);
        col.setRenderer(textRenderer(schemaField));
        final ColumnSetup setup = columnCustomization.get(schemaField.name());
        if (setup != null) {
            setup.setUp(col, schemaField, valueProvider);
        }
    }
}
