// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.validation;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.diaspore.TransformInput;
import com.bytefacets.diaspore.TransformOutput;
import com.bytefacets.diaspore.schema.ChangedFieldSet;
import com.bytefacets.diaspore.schema.Field;
import com.bytefacets.diaspore.schema.FieldList;
import com.bytefacets.diaspore.schema.Metadata;
import com.bytefacets.diaspore.schema.Schema;
import com.bytefacets.diaspore.schema.SchemaField;
import com.bytefacets.diaspore.schema.TypeId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ValidationOperator {
    private final Map<Integer, KeyedRow> rowToData = new HashMap<>();
    private final String[] keyFields;
    private final Set<String> valueFields = new LinkedHashSet<>();
    private final Input input;
    private final AtomicBoolean activeValidation = new AtomicBoolean(false);

    public ValidationOperator(final String[] keyFields, final String... valueFields) {
        this.keyFields = keyFields;
        this.valueFields.addAll(List.of(valueFields));
        this.input = new Input();
    }

    public Validation expect() {
        if(activeValidation.getAndSet(true)) {
            throw new IllegalStateException("Another validation is currently active, cannot create a new one");
        }
        return new Validation(this, new ChangeSet(), activeValidation);
    }

    public void assertNoActiveValidation() {
        if(activeValidation.get()) {
            throw new IllegalStateException("A validation was started, but validate() was never called");
        }
    }

    public List<String> getValidationErrors(final ChangeSet expectation) {
        return new Validator(input.currentChangeSet, expectation, input.calculatedActiveRows, input.source.rowProvider()).validate();
    }

    public void validate(final ChangeSet expectation) {
        final var errors = getValidationErrors(expectation);
        if (!errors.isEmpty()) {
            throw new ValidationException(errors);
        }
    }

    public void validateNoChanges() {
        validate(new ChangeSet());
    }

    public TransformInput input() {
        return input;
    }

    public void clearChanges() {
        input.currentChangeSet = new ChangeSet();
    }

    private class Input implements TransformInput {
        private final Set<Integer> calculatedActiveRows = new TreeSet<>();
        private ChangeSet currentChangeSet = new ChangeSet();
        private Schema schema;
        private TransformOutput source;

        @Override
        public void setSource(final TransformOutput output) {
            this.source = output;
        }

        @Override
        public void schemaUpdated(final Schema schema) {
            this.schema = schema;
            if (schema != null) {
                currentChangeSet.schema(toMap(schema.fields()));
                currentChangeSet.metadata(toMetadataMap(schema.fields()));
            } else {
                currentChangeSet.nullSchema();
                calculatedActiveRows.clear();
            }
        }

        @Override
        public void rowsAdded(final IntIterable rows) {
            rows.forEach(
                    rowId -> {
                        final var keyedRow = new KeyedRow(toKey(rowId), toRowData(rowId, null));
                        currentChangeSet.added(keyedRow.key, keyedRow.data);
                        final var oldKeyedRow = rowToData.put(rowId, keyedRow);
                        if (oldKeyedRow != null) {
                            currentChangeSet.error(String.format("Duplicate row added: %d", rowId));
                        }
                        if(!calculatedActiveRows.add(rowId)) {
                            currentChangeSet.error(String.format("Received add for row that was already active: %d", rowId));
                        }
                    });
        }

        @Override
        public void rowsChanged(final IntIterable rows, final ChangedFieldSet changedFields) {
            rows.forEach(
                    rowId -> {
                        final var partialRow =
                                new KeyedRow(toKey(rowId), toRowData(rowId, changedFields));
                        currentChangeSet.changed(partialRow.key, partialRow.data);

                        final var oldKeyedRow = rowToData.get(rowId);
                        if (oldKeyedRow == null) {
                            currentChangeSet.error(
                                    String.format("Changed row not found: %d", rowId));
                        } else {
                            rowToData.put(rowId, oldKeyedRow.update(partialRow));
                        }
                        if(!calculatedActiveRows.contains(rowId)) {
                            currentChangeSet.error(String.format("Row was not active: %d", rowId));
                        }
                    });
        }

        @Override
        public void rowsRemoved(final IntIterable rows) {
            rows.forEach(
                    rowId -> {
                        final var keyedRow = rowToData.remove(rowId);
                        if (keyedRow == null) {
                            currentChangeSet.error(
                                    String.format("Removed row not found: %d", rowId));
                        } else {
                            currentChangeSet.removed(keyedRow.key());
                        }
                        if(!calculatedActiveRows.remove(rowId)) {
                            currentChangeSet.error(String.format("Row was not active: %d", rowId));
                        }
                    });
        }

        private Key toKey(final int rowId) {
            final List<Object> key = new ArrayList<>(keyFields.length);
            for (String name : keyFields) {
                final SchemaField sField = schema.fields().field(name);
                key.add(sField.field().objectValueAt(rowId));
            }
            return new Key(key);
        }

        private RowData toRowData(final int rowId, final ChangedFieldSet changedFields) {
            final Map<String, Object> data = new HashMap<>();
            for (String name : valueFields) {
                final SchemaField sField = schema.fields().field(name);
                final boolean include =
                        changedFields == null || changedFields.isChanged(sField.fieldId());
                if (include) {
                    final var value = sField.field().objectValueAt(rowId);
                    data.put(name, value);
                }
            }
            return new RowData(data);
        }
    }

    private Map<String, Class<?>> toMap(final FieldList fields) {
        final Map<String, Class<?>> map = new HashMap<>();
        for (String name : keyFields) {
            final Field field = fields.field(name).field();
            map.put(name, TypeId.toClass(field.typeId()));
        }
        for (String name : valueFields) {
            final Field field = fields.field(name).field();
            map.put(name, TypeId.toClass(field.typeId()));
        }
        return map;
    }

    private Map<String, Metadata> toMetadataMap(final FieldList fields) {
        final Map<String, Metadata> map = new HashMap<>();
        for (String name : keyFields) {
            final SchemaField field = fields.field(name);
            if(field.metadata() != null) {
                map.put(name, field.metadata());
            }
        }
        for (String name : valueFields) {
            final SchemaField field = fields.field(name);
            if(field.metadata() != null) {
                map.put(name, field.metadata());
            }
        }
        return map;
    }

    private record KeyedRow(Key key, RowData data) {
        private KeyedRow update(final KeyedRow partial) {
            return new KeyedRow(partial.key, data.replace(partial.data().data()));
        }
    }
}
