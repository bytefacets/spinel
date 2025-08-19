// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.validation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public record RowData(Map<String, Object> data) {

    public RowData replace(final Map<String, Object> updated) {
        final Map<String, Object> newData = new HashMap<>(data);
        newData.putAll(updated);
        return new RowData(newData);
    }

    public Key keyFrom(final String[] keyFields) {
        final List<Object> keyValues = new ArrayList<>(keyFields.length);
        for (String key : keyFields) {
            keyValues.add(data.get(key));
        }
        return new Key(keyValues);
    }

    public static RowDataTemplate template(final String... fieldNames) {
        return new RowDataTemplate(List.of(fieldNames));
    }

    public static final class RowDataTemplate {
        private final List<String> fieldNames;

        private RowDataTemplate(final List<String> fieldNames) {
            this.fieldNames = fieldNames;
        }

        public RowData rowData(final Object... values) {
            final Map<String, Object> data = new HashMap<>();
            for(int i = 0, len = Math.min(values.length, fieldNames.size()); i < len; i++) {
                if(values[i] != null) {
                    data.put(fieldNames.get(i), values[i]);
                }
            }
            return new RowData(data);
        }
    }
}
