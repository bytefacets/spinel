// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.common.jexl;

import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.exception.FieldNotFoundException;
import com.bytefacets.diaspore.schema.Cast;
import com.bytefacets.diaspore.schema.Field;
import com.bytefacets.diaspore.schema.FieldResolver;
import com.bytefacets.diaspore.schema.SchemaBindable;
import com.bytefacets.diaspore.schema.TypeId;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlScript;

public final class JexlRowContext implements SchemaBindable, JexlContext {
    private final Map<String, Field> fieldMap = new HashMap<>(4);
    private final JexlScript script;
    private int currentRow;

    public static JexlRowContext jexlRowContext(final JexlScript script) {
        return new JexlRowContext(script);
    }

    JexlRowContext(final JexlScript script) {
        this.script = requireNonNull(script, "script");
    }

    @Override
    public void bindToSchema(final FieldResolver fieldResolver) {
        extractFields(fieldResolver);
        currentRow = -1;
    }

    @Override
    public void unbindSchema() {
        fieldMap.clear();
        currentRow = -1;
    }

    public String source() {
        return script.getSourceText();
    }

    public void setCurrentRow(final int currentRow) {
        this.currentRow = currentRow;
    }

    @Override
    public Object get(final String name) {
        final Field field = fieldMap.get(name);
        if (field == null) {
            throw new FieldNotFoundException(
                    String.format(
                            "Failed processing expression (%s): Unknown field '%s'",
                            source(), name));
        }
        return field.objectValueAt(currentRow);
    }

    @Override
    public void set(final String name, final Object value) {
        throw new UnsupportedOperationException(
                "Read-only context, source=" + script.getSourceText());
    }

    @Override
    public boolean has(final String name) {
        return fieldMap.containsKey(name);
    }

    private void extractFields(final FieldResolver fieldResolver) {
        final String[] unboundParameters = script.getUnboundParameters();
        if (unboundParameters != null) {
            for (String fieldName : unboundParameters) {
                register(fieldResolver, fieldName);
            }
        }
        for (var varList : script.getVariables()) {
            for (var fieldName : varList) {
                register(fieldResolver, fieldName);
            }
        }
    }

    private void register(final FieldResolver fieldResolver, final String fieldName) {
        final Field field = fieldResolver.getField(fieldName);
        // jexl for some reason doesn't like char
        if (field.typeId() == TypeId.Char) {
            fieldMap.put(fieldName, Cast.toStringField(field));
        } else {
            fieldMap.put(fieldName, field);
        }
    }

    // VisibleForTesting
    Object evaluate(final int row) {
        currentRow = row;
        return script.execute(this);
    }

    // VisibleForTesting
    Set<String> boundFields() {
        return fieldMap.keySet();
    }
}
