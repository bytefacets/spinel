// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.filter;

import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.schema.FieldResolver;
import java.util.Arrays;
import java.util.Objects;

/**
 * A RowPredicate implementation that allows the row to pass only if all RowPredicates return true.
 */
public final class AndPredicate implements RowPredicate {
    private final RowPredicate[] predicates;

    public static AndPredicate andPredicate(final RowPredicate[] predicates) {
        return new AndPredicate(predicates);
    }

    AndPredicate(final RowPredicate[] predicates) {
        this.predicates = requireNonNull(predicates, "predicates");
    }

    @Override
    public boolean testRow(final int row) {
        for (RowPredicate predicate : predicates) {
            if (!predicate.testRow(row)) {
                return false;
            }
        }
        return true;
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
        final AndPredicate that = (AndPredicate) o;
        return Objects.deepEquals(predicates, that.predicates);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(predicates);
    }
}
