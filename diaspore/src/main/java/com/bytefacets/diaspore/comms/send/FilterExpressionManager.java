// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.comms.send;

import static com.bytefacets.diaspore.filter.JexlRowPredicate.jexlPredicate;
import static com.bytefacets.diaspore.filter.OrPredicate.orPredicate;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.arrays.GenericArray;
import com.bytefacets.collections.functional.GenericConsumer;
import com.bytefacets.collections.hash.StringGenericIndexedMap;
import com.bytefacets.diaspore.comms.subscription.ChangeDescriptor;
import com.bytefacets.diaspore.comms.subscription.ChangeDescriptorFactory;
import com.bytefacets.diaspore.comms.subscription.ModificationRequest;
import com.bytefacets.diaspore.filter.Filter;
import com.bytefacets.diaspore.filter.RowPredicate;
import org.apache.commons.jexl3.JexlEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages receipt and application of ChangeDescriptors to a Filter by reference counting
 * expressions and updating an OR predicate and applying it when new expressions are applied, or
 * when expressions are fully dereferenced.
 */
public final class FilterExpressionManager implements ModificationHandler {
    private static final Logger log = LoggerFactory.getLogger(FilterExpressionManager.class);
    private final StringGenericIndexedMap<PredicateHolder> expressionPredicates =
            new StringGenericIndexedMap<>(4);
    private final Filter filter;
    private final JexlEngine jexlEngine;
    private final PredicateBuilder predicateBuilder = new PredicateBuilder();
    private final String logPrefix;

    public static FilterExpressionManager filterExpressionManager(
            final Filter filter, final JexlEngine jexlEngine, final String logPrefix) {
        return new FilterExpressionManager(filter, jexlEngine, logPrefix);
    }

    private FilterExpressionManager(
            final Filter filter, final JexlEngine jexlEngine, final String logPrefix) {
        this.filter = requireNonNull(filter, "filter");
        this.jexlEngine = requireNonNull(jexlEngine, "jexlEngine");
        this.logPrefix = requireNonNull(logPrefix, "logPrefix");
    }

    @Override
    public ModificationResponse apply(final ModificationRequest modificationRequest) {
        final ChangeDescriptor changeDescriptor = (ChangeDescriptor) modificationRequest;
        final String expression = changeDescriptor.arguments()[0].toString();
        return switch (changeDescriptor.action()) {
            case ChangeDescriptorFactory.Action.ADD -> add(expression);
            case ChangeDescriptorFactory.Action.REMOVE -> remove(expression);
            default -> notUnderstood(changeDescriptor.action());
        };
    }

    private ModificationResponse add(final String expression) {
        final int sizeBefore = expressionPredicates.size();
        final int entry = expressionPredicates.add(expression);
        final int sizeAfter = expressionPredicates.size();
        if (sizeBefore != sizeAfter) {
            log.debug("{} Adding expression: {}", logPrefix, expression);
            final RowPredicate predicate = jexlPredicate(jexlEngine, expression);
            expressionPredicates.putValueAt(entry, new PredicateHolder(predicate));
            return rebuildFinalPredicate();
        } else {
            final int newCount = expressionPredicates.getValueAt(entry).increment();
            updatedRefCount(expression, newCount);
            return ModificationResponse.SUCCESS;
        }
    }

    private ModificationResponse remove(final String expression) {
        final int entry = expressionPredicates.lookupEntry(expression);
        if (entry != -1) {
            final PredicateHolder refCounter = expressionPredicates.getValueAt(entry);
            final int newCount = refCounter.decrement();
            if (newCount == 0) {
                log.debug("{} Removing expression: {}", logPrefix, expression);
                expressionPredicates.removeAt(entry);
                return rebuildFinalPredicate();
            } else {
                updatedRefCount(expression, newCount);
            }
            return ModificationResponse.SUCCESS;
        } else {
            return expressionNotFound(expression);
        }
    }

    private ModificationResponse rebuildFinalPredicate() {
        predicateBuilder.reset(expressionPredicates.size());
        expressionPredicates.forEachValue(predicateBuilder);
        filter.updatePredicate(predicateBuilder.build());
        return ModificationResponse.SUCCESS;
    }

    private void updatedRefCount(final String expression, final int newCount) {
        log.debug(
                "{} Updating reference count to {} on expression: {}",
                logPrefix,
                newCount,
                expression);
    }

    /** Builder which manages the predicate iteration with some state. */
    private static final class PredicateBuilder implements GenericConsumer<PredicateHolder> {
        private RowPredicate[] predicates;
        private int index = 0;

        private void reset(final int size) {
            predicates = GenericArray.create(RowPredicate.class, size);
            index = 0;
        }

        private RowPredicate build() {
            return orPredicate(predicates);
        }

        @Override
        public void accept(final PredicateHolder predicateHolder) {
            predicates[index++] = predicateHolder.predicate;
        }
    }

    /**
     * Small holder object to save on reconstructing each predicate and tracking a reference count.
     */
    private static final class PredicateHolder {
        private final RowPredicate predicate;
        private int referenceCount;

        private PredicateHolder(final RowPredicate predicate) {
            this.predicate = requireNonNull(predicate, "predicate");
            this.referenceCount = 1;
        }

        private int increment() {
            return ++referenceCount;
        }

        private int decrement() {
            return --referenceCount;
        }
    }

    private static ModificationResponse notUnderstood(final String action) {
        return new ModificationResponse(false, "Modification not understood: " + action, null);
    }

    private static ModificationResponse expressionNotFound(final String expression) {
        return new ModificationResponse(false, "Expression not found: " + expression, null);
    }
}
