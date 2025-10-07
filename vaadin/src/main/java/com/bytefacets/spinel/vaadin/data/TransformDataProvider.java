// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.vaadin.data;

import static com.bytefacets.spinel.common.Connector.connectInputToOutput;
import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.comms.send.ModificationResponse;
import com.bytefacets.spinel.comms.send.SubscriptionContainer;
import com.bytefacets.spinel.comms.subscription.ModificationRequest;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.ui.Pager;
import com.vaadin.flow.data.provider.AbstractDataProvider;
import com.vaadin.flow.data.provider.Query;
import io.netty.channel.EventLoop;
import java.io.Serial;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adapter for a TransformOutput to a Vaadin DataProvider. The provider uses a TransformConsumer to
 * collect data changes from the TransformOutput. When the DataProvider access methods (size or
 * fetch) are called, the DataProvider tells the TransformConsumer to report added and removed rows
 * to the UI thread.
 */
public final class TransformDataProvider
        extends AbstractDataProvider<TransformRow, ModificationRequest> {
    private static final Logger log = LoggerFactory.getLogger(TransformDataProvider.class);
    // because the DataProvider is Serializable :(
    private @Serial static final long serialVersionUID = 1L;
    private final transient SubscriptionContainer subscription;
    private final transient UIThreadConsumer uiThreadConsumer;
    private final transient TransformConsumer eventLoopConsumer;
    private final transient EventLoop eventLoop;
    private transient ModificationRequest currentFilter = null;

    /**
     * Factory method for this DataProvider.
     *
     * @param subscription the subscription producing data for this DataProvider
     * @param pager a pager instance (user-provided to accommodate user-specified sizing)
     * @param schemaConsumer callback for schema processing on the UI thread
     * @param eventLoop the event loop that is handling the data processing to manage connection and
     *     disconnection
     * @return the provider
     */
    public static TransformDataProvider transformDataProvider(
            final SubscriptionContainer subscription,
            final Pager pager,
            final Consumer<Schema> schemaConsumer,
            final EventLoop eventLoop) {
        final UIThreadConsumer uiThreadConsumer = new UIThreadConsumer(schemaConsumer, pager);
        final TransformConsumer eventLoopConsumer = new TransformConsumer(uiThreadConsumer);
        return new TransformDataProvider(
                subscription, eventLoopConsumer, uiThreadConsumer, eventLoop);
    }

    TransformDataProvider(
            final SubscriptionContainer subscription,
            final TransformConsumer eventLoopConsumer,
            final UIThreadConsumer uiThreadConsumer,
            final EventLoop eventLoop) {
        this.subscription = subscription;
        this.eventLoop = requireNonNull(eventLoop, "eventLoop");
        this.eventLoopConsumer = requireNonNull(eventLoopConsumer, "eventLoopConsumer");
        this.uiThreadConsumer = requireNonNull(uiThreadConsumer, "uiThreadConsumer");
        eventLoop.execute(() -> connectInputToOutput(eventLoopConsumer, subscription));
    }

    public void disconnect() {
        eventLoop.execute(
                () -> {
                    log.debug("Terminating subscription");
                    subscription.output().detachInput(eventLoopConsumer);
                    subscription.terminateSubscription();
                });
    }

    @Override
    public boolean isInMemory() {
        return true;
    }

    @Override
    public int size(final Query<TransformRow, ModificationRequest> query) {
        eventLoopConsumer.applyOnUiThread();
        applyFilterIfChanged(query.getFilter().orElse(null));
        return uiThreadConsumer.rowCount();
    }

    @Override
    public Stream<TransformRow> fetch(final Query<TransformRow, ModificationRequest> query) {
        eventLoopConsumer.applyOnUiThread();
        applyFilterIfChanged(query.getFilter().orElse(null));
        final Stream.Builder<TransformRow> streamBuilder = Stream.builder();
        uiThreadConsumer.rowsInRange(
                query.getOffset(),
                query.getLimit(),
                (position, row) -> streamBuilder.accept(new TransformRow(row)));
        return streamBuilder.build();
    }

    // VisibleForTesting
    void applyFilterIfChanged(final ModificationRequest queryFilter) {
        if (!Objects.equals(queryFilter, currentFilter)) {
            final CountDownLatch latch = new CountDownLatch(1);
            eventLoop.execute(
                    () -> {
                        final boolean success = applyFilterOnEventLoop(currentFilter, queryFilter);
                        latch.countDown();
                        currentFilter = success ? queryFilter : null;
                    });
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private boolean applyFilterOnEventLoop(
            final ModificationRequest removeFilter, final ModificationRequest addFilter) {
        if (removeFilter != null) {
            subscription.remove(removeFilter);
        }
        if (addFilter != null) {
            final ModificationResponse response = subscription.add(addFilter);
            if (!response.success()) {
                log.warn("Filter request failed: {}", response.message(), response.exception());
            }
            return response.success();
        }
        return true;
    }

    // VisibleForTesting
    ModificationRequest currentFilter() {
        return currentFilter;
    }
}
