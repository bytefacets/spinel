// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.filter;

import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.schema.FieldResolver;
import java.util.Arrays;
import java.util.Objects;

/**
 * A RowPredicate implementation that allows the row to pass only if one of RowPredicates return
 * true.
 */
public final class OrPredicate implements RowPredicate {
    private final RowPredicate[] predicates;

    public static OrPredicate orPredicate(final RowPredicate[] predicates) {
        return new OrPredicate(predicates);
    }

    OrPredicate(final RowPredicate[] predicates) {
        this.predicates = requireNonNull(predicates, "predicates");
    }

    @Override
    public boolean testRow(final int row) {
        for (RowPredicate predicate : predicates) {
            if (predicate.testRow(row)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void bindToSchema(final FieldResolver fieldResolver) {
        for (RowPredicate predicate : predicates) {
            predicate.bindToSchema(fieldResolver);
        }
    }

    @Override
    public void unbindSchema() {
        for (RowPredicate predicate : predicates) {
            predicate.unbindSchema();
        }
    }

    @SuppressWarnings("NeedBraces")
    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final OrPredicate that = (OrPredicate) o;
        return Objects.deepEquals(predicates, that.predicates);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(predicates);
    }
}
