// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.validation;

import com.bytefacets.spinel.RowProvider;
import com.bytefacets.spinel.schema.Metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

class Validator {
    private final List<String> errors = new ArrayList<>();
    private final ChangeSet observed;
    private final ChangeSet expected;
    private final Set<Integer> calculatedActiveRows;
    private final RowProvider rowProvider;

    Validator(final ChangeSet observed, final ChangeSet expected, final Set<Integer> calculatedActiveRows, final RowProvider rowProvider) {
        this.observed = observed;
        this.expected = expected;
        this.calculatedActiveRows = new TreeSet<>(calculatedActiveRows);
        this.rowProvider = rowProvider;
    }

    List<String> validate() {
        validateSchema(observed.nullSchema, expected.nullSchema);
        validateSchema(observed.schema, expected.schema);
        validate("Added", observed.added, expected.added);
        validate("Changed", observed.changed, expected.changed);
        validateRemoved(observed.removed, expected.removed);
        validateRowProvider();
        return errors;
    }

    private void validateRowProvider() {
        final Set<Integer> alreadySeen = new HashSet<>();
        rowProvider.forEach(row -> {
            if(!alreadySeen.add(row)) {
                errors.add(String.format("Row was provided multiple times from rowProvider: %d", row));
            } else if(!calculatedActiveRows.remove(row)) {
                errors.add(String.format("Row active in rowProvider, but not from events: %d", row));
            }
        });
        for(int row : calculatedActiveRows) {
            errors.add(String.format("Expected row active, but row was not provided from rowProvider: %d", row));
        }
    }

    private void validateSchema(
            final Map<String, Class<?>> observed, final Map<String, Class<?>> expected) {
        for (var expectedField : expected.entrySet()) {
            final String fieldName = expectedField.getKey();
            final Class<?> expectedType = expectedField.getValue();
            final Class<?> observedType = observed.get(fieldName);
            if (observedType == null) {
                missingExpectedField(fieldName, expectedType);
            } else if (!observedType.equals(expectedType)) {
                unexpectedFieldType(fieldName, expectedType, observedType);
            }
        }
    }

    private void validateFieldMetadata(
            final Map<String, Metadata> observed, final Map<String, Metadata> expected) {
        for (var expectedField : expected.entrySet()) {
            final String fieldName = expectedField.getKey();
            final Metadata expectedMd = expectedField.getValue();
            final Metadata observedMd = observed.get(fieldName);
            if (observedMd == null) {
                missingExpectedFieldMetadata(fieldName, expectedMd);
            } else if (!observedMd.equals(expectedMd)) {
                unexpectedFieldMetadata(fieldName, expectedMd, observedMd);
            }
        }
    }
    private void validateSchema(final boolean observed, final boolean expected) {
        if (observed != expected) {
            errors.add(
                    String.format(
                            "Schema: Observed %b null schema, but expected %b",
                            observed, expected));
        }
    }

    private void validate(
            final String action,
            final Map<Key, RowData> observed,
            final Map<Key, RowData> expected) {
        final Set<Key> allKeys = new HashSet<>(observed.keySet());
        allKeys.addAll(expected.keySet());
        for (var key : allKeys) {
            final RowData obs = observed.get(key);
            final RowData exp = expected.get(key);
            if (obs == null) {
                missingExpectedKey(action, key);
            } else if (exp == null) {
                unexpectedKey(action, key);
            } else if (!Objects.equals(obs, exp)) {
                dataMismatch(action, key, diff(obs.data(), exp.data()));
            }
        }
    }

    private void dataMismatch(final String action, final Key key, final Map<String, Diff> diff) {
        errors.add(String.format("%s: data mismatch for %s: %s", action, key, diff));
    }

    private void unexpectedKey(final String action, final Key key) {
        errors.add(String.format("%s: observed %s, but did not expect it", action, key));
    }

    private void missingExpectedKey(final String action, final Key key) {
        errors.add(String.format("%s: expected %s, but did not observe it", action, key));
    }

    private void unexpectedFieldType(
            final String name, final Class<?> expectedType, final Class<?> observedType) {
        errors.add(
                String.format(
                        "Schema: expected %s field %s but was of type %s",
                        expectedType.getSimpleName(), name, observedType.getSimpleName()));
    }

    private void missingExpectedField(final String name, final Class<?> type) {
        errors.add(String.format("Schema: expected %s field %s", type.getSimpleName(), name));
    }

    private void unexpectedFieldMetadata(
            final String name, final Metadata expectedMd, final Metadata observedMd) {
        errors.add(String.format("Schema: expected %s for field %s but was %s", expectedMd, name, observedMd));
    }

    private void missingExpectedFieldMetadata(final String name, final Metadata md) {
        errors.add(String.format("Metadata: expected %s for field %s", md, name));
    }

    private Map<String, Diff> diff(
            final Map<String, Object> observed, final Map<String, Object> expected) {
        final Map<String, Diff> result = new HashMap<>();
        final Set<String> allKeys = new HashSet<>(observed.keySet());
        allKeys.addAll(expected.keySet());
        for (String key : allKeys) {
            final var obs = observed.get(key);
            final var exp = expected.get(key);
            if (!Objects.equals(obs, exp)) {
                result.put(key, new Diff(obs, exp));
            }
        }
        return result;
    }

    private void validateRemoved(final Set<Key> observed, final Set<Key> expected) {
        final Set<Key> all = new HashSet<>(observed);
        all.addAll(expected);
        for (Key key : all) {
            if (!observed.contains(key)) {
                errors.add(String.format("Remove: expected %s but did not observe", key));
            } else if (!expected.contains(key)) {
                errors.add(String.format("Remove: observed %s but did not expected", key));
            }
        }
    }

    private record Diff(Object observed, Object expected) {}
}
