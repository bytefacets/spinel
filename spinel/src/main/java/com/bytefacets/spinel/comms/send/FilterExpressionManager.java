// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.comms.send;

import static com.bytefacets.spinel.filter.JexlRowPredicate.jexlPredicate;
import static com.bytefacets.spinel.filter.OrPredicate.orPredicate;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.arrays.GenericArray;
import com.bytefacets.collections.functional.GenericConsumer;
import com.bytefacets.collections.hash.StringGenericIndexedMap;
import com.bytefacets.spinel.comms.subscription.ModificationRequest;
import com.bytefacets.spinel.filter.Filter;
import com.bytefacets.spinel.filter.RowPredicate;
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
    private static final ModificationResponse NOT_UNDERSTOOD =
            new ModificationResponse(
                    false, "Modification not understood by FilterExpressionManager", null);
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
    public ModificationResponse add(final ModificationRequest modificationRequest) {
        final String expression = toExpression(modificationRequest);
        if (expression != null) {
            return add(expression);
        } else {
            return NOT_UNDERSTOOD;
        }
    }

    @Override
    public ModificationResponse remove(final ModificationRequest modificationRequest) {
        final String expression = toExpression(modificationRequest);
        if (expression != null) {
            return remove(expression);
        } else {
            return NOT_UNDERSTOOD;
        }
    }

    private String toExpression(final ModificationRequest modificationRequest) {
        final Object[] args = modificationRequest.arguments();
        if (args != null && args.length >= 1) {
            return args[0].toString();
        }
        return null;
    }

    private ModificationResponse add(final String expression) {
        final int sizeBefore = expressionPredicates.size();
        final int entry = expressionPredicates.add(expression);
        final int sizeAfter = expressionPredicates.size();
        if (sizeBefore != sizeAfter) {
            log.debug("{} Adding expression: {}", logPrefix, expression);
            try {
                final RowPredicate predicate = jexlPredicate(jexlEngine, expression);
                expressionPredicates.putValueAt(entry, new PredicateHolder(predicate));
            } catch (RuntimeException ex) {
                log.warn("{} Failed adding expression: {}", logPrefix, expression, ex);
                expressionPredicates.removeAt(entry); // cleanup, we couldn't add it
                throw ex;
            }
            return rebuildFinalPredicate();
        } else {
            final int newCount = expressionPredicates.getValueAt(entry).increment();
            log.debug(
                    "{} Increasing reference count to {} on expression: {}",
                    logPrefix,
                    newCount,
                    expression);
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
                log.debug(
                        "{} Reducing reference count to {} on expression: {}",
                        logPrefix,
                        newCount,
                        expression);
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
            predicates = size != 0 ? GenericArray.create(RowPredicate.class, size) : null;
            index = 0;
        }

        private RowPredicate build() {
            return predicates == null ? null : orPredicate(predicates);
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

    private static ModificationResponse expressionNotFound(final String expression) {
        return new ModificationResponse(false, "Expression not found: " + expression, null);
    }

    // VisibleForTesting
    int expressionCount() {
        return expressionPredicates.size();
    }
}
