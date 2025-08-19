// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.projection;

import static com.bytefacets.spinel.exception.OperatorSetupException.setupException;
import static java.util.Objects.requireNonNullElse;

import com.bytefacets.collections.functional.StringConsumer;
import com.bytefacets.collections.hash.StringIndexedSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class FieldSorter {
    private static final String[] EMPTY = new String[0];
    private final String[] left;
    private final String[] right;
    private final StringIndexedSet fieldSet = new StringIndexedSet(16);

    FieldSorter(final String[] left, final String[] right) {
        this.left = requireNonNullElse(left, EMPTY);
        this.right = requireNonNullElse(right, EMPTY);
    }

    void validateUniqueNames(final String projectionName) {
        if (left.length == 0 && right.length == 0) {
            return;
        }
        final Set<String> nonUniqueNames = new HashSet<>(2);
        for (String name : left) {
            if (!addUnique(name)) {
                nonUniqueNames.add(name);
            }
        }
        for (String name : right) {
            if (!addUnique(name)) {
                nonUniqueNames.add(name);
            }
        }
        fieldSet.clear();
        if (!nonUniqueNames.isEmpty()) {
            throw setupException(
                    String.format(
                            "Exception setting up projection %s; "
                                    + "non-uniqueNames in ordering criteria: %s",
                            projectionName, nonUniqueNames));
        }
    }

    void rebuild(final StringConsumer nameConsumer, final List<Iterable<String>> fieldNames) {
        if (left.length == 0 && right.length == 0) {
            for (var it : fieldNames) {
                it.forEach(nameConsumer::accept);
            }
            return;
        }
        fieldSet.clear();
        for (String name : left) {
            if (addUnique(name)) {
                nameConsumer.accept(name);
            }
        }

        for (String name : right) {
            fieldSet.add(name);
        }

        for (var it : fieldNames) {
            it.forEach(
                    name -> {
                        if (addUnique(name)) {
                            nameConsumer.accept(name);
                        }
                    });
        }

        for (String name : right) {
            nameConsumer.accept(name);
        }
    }

    private boolean addUnique(final String name) {
        final int before = fieldSet.size();
        fieldSet.add(name);
        return before != fieldSet.size();
    }
}
