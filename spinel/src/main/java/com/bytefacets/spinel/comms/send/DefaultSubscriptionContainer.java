// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.comms.send;

import static com.bytefacets.spinel.comms.send.FilterExpressionManager.filterExpressionManager;
import static com.bytefacets.spinel.filter.FilterBuilder.filter;
import static com.bytefacets.spinel.projection.ProjectionBuilder.projection;
import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.TransformInput;
import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.common.Connector;
import com.bytefacets.spinel.common.jexl.JexlEngineProvider;
import com.bytefacets.spinel.comms.SubscriptionConfig;
import com.bytefacets.spinel.comms.subscription.ModificationRequest;
import com.bytefacets.spinel.comms.subscription.ModificationRequestFactory;
import com.bytefacets.spinel.filter.Filter;
import com.bytefacets.spinel.projection.Projection;
import java.util.List;

/**
 * A basic container which provides, per-subscription, a Filter and an optional Projection.
 *
 * <p>The filter is customizable by jexl expressions that operate over each row. The expressions are
 * applied as an OrPredicate, meaning that a row will pass if ANY of the expressions pass.
 *
 * <p>The projection is added if the SubscriptionConfig specifies a field selection.
 *
 * @see FilterExpressionManager
 */
public final class DefaultSubscriptionContainer implements SubscriptionContainer {
    private final TransformOutput source;
    private final TransformInput input;
    private final TransformOutput output;
    private final ModificationHandler modificationHandler;

    public static DefaultSubscriptionContainer defaultSubscriptionContainer(
            final ConnectedSessionInfo sessionInfo,
            final SubscriptionConfig subscriptionConfig,
            final List<ModificationRequest> initialModifications,
            final TransformOutput output,
            final ModificationHandlerRegistry modificationHandler) {
        return new DefaultSubscriptionContainer(
                sessionInfo, subscriptionConfig, initialModifications, output, modificationHandler);
    }

    public static DefaultSubscriptionContainer defaultSubscriptionContainer(
            final CommonSubscriptionContext context) {
        return new DefaultSubscriptionContainer(
                context.sessionInfo(),
                context.subscriptionConfig(),
                context.initialModifications(),
                context.output(),
                context.modificationHandler());
    }

    private DefaultSubscriptionContainer(
            final ConnectedSessionInfo sessionInfo,
            final SubscriptionConfig subscriptionConfig,
            final List<ModificationRequest> initialModifications,
            final TransformOutput output,
            final ModificationHandlerRegistry modificationHandler) {
        requireNonNull(subscriptionConfig, "subscriptionConfig");
        requireNonNull(sessionInfo, "sessionInfo");
        this.source = requireNonNull(output, "output");
        this.modificationHandler = requireNonNull(modificationHandler, "modificationHandler");

        final Filter filter = createFilter(subscriptionConfig);
        this.input = filter.input();

        // register filter management
        final FilterExpressionManager expressionMgr =
                filterExpressionManager(
                        filter, JexlEngineProvider.defaultJexlEngine(), sessionInfo.toString());
        modificationHandler.register(ModificationRequestFactory.Target.FILTER, expressionMgr);

        // inject projection if necessary
        if (!subscriptionConfig.fields().isEmpty()) {
            final Projection projection = createProjection(subscriptionConfig);
            Connector.connectOutputToInput(filter, projection);
            this.output = projection.output();
        } else {
            this.output = filter.output();
        }

        //
        applyModifications(modificationHandler, initialModifications);

        // do last for one pass over the rows
        Connector.connectOutputToInput(source, filter);
    }

    private void applyModifications(
            final ModificationHandlerRegistry modificationHandler,
            final List<ModificationRequest> initialModifications) {
        for (ModificationRequest request : initialModifications) {
            modificationHandler.add(request);
        }
    }

    private Projection createProjection(final SubscriptionConfig subscriptionConfig) {
        return projection().include(subscriptionConfig.fields().toArray(String[]::new)).build();
    }

    private Filter createFilter(final SubscriptionConfig config) {
        return filter().passesWhenNoPredicate(config.defaultAll()).build();
    }

    @Override
    public ModificationResponse add(final ModificationRequest modificationRequest) {
        return modificationHandler.add(modificationRequest);
    }

    @Override
    public ModificationResponse remove(final ModificationRequest modificationRequest) {
        return modificationHandler.remove(modificationRequest);
    }

    @Override
    public void terminateSubscription() {
        source.detachInput(input);
    }

    @Override
    public TransformOutput output() {
        return output;
    }
}
