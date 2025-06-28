<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.schema;

import com.bytefacets.collections.hash.StringGenericIndexedMap;
<#list types as type>
import com.bytefacets.collections.store.${type.name}ChunkMatrixStore;
import com.bytefacets.collections.store.${type.name}MatrixStore;
</#list>

import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class MatrixStoreFieldFactory {
    private final int initialSize;
    private final int chunkSize;
    private final FieldChangeListener listener;

    private MatrixStoreFieldFactory(final int initialSize, final int chunkSize, final FieldChangeListener listener) {
        this.initialSize = initialSize;
        this.chunkSize = chunkSize;
        this.listener = Objects.requireNonNull(listener, "listener");
    }

    public static MatrixStoreFieldFactory matrixStoreFieldFactory(final int initialSize, final int chunkSize, final FieldChangeListener listener) {
        return new MatrixStoreFieldFactory(initialSize, chunkSize, listener);
    }

    public FieldList createFieldList(final Map<Byte, List<FieldDescriptor>> fieldDescriptors) {
        final int size = fieldDescriptors.values().stream().mapToInt(List::size).sum();
        return createFieldList(new StringGenericIndexedMap<>(size, 1f), fieldDescriptors);
    }

    public FieldList createFieldList(final StringGenericIndexedMap<SchemaField> fieldMap, final Map<Byte, List<FieldDescriptor>> fieldDescriptors) {
        fieldDescriptors.forEach((typeId, fieldsForType) -> {
            switch(typeId) {
<#list types as type>
                case TypeId.${type.name} -> add${type.name}Fields(fieldMap, fieldsForType);
</#list>
                default -> throw new RuntimeException();
            }
        });
        return FieldList.fieldList(fieldMap);
    }

<#list types as type>
    private void add${type.name}Fields(final StringGenericIndexedMap<SchemaField> fieldMap, final List<FieldDescriptor> fieldDescriptors) {
        final int count = fieldDescriptors.size();
        final var store = new ${type.name}ChunkMatrixStore${type.instanceGenerics}(initialSize, chunkSize, count);
        for(int i = 0; i < count; i++) {
            final FieldDescriptor fd = fieldDescriptors.get(i);
            final int fieldId = fieldMap.add(fd.name());
            final var field = ${type.name?lower_case}FieldBinding(store, i, fieldId);
            fieldMap.putValueAt(fieldId, SchemaField.schemaField(fieldId, fd.name(), field, fd.metadata()));
        }
    }

    private ${type.name}WritableField ${type.name?lower_case}FieldBinding(final ${type.name}MatrixStore${type.instanceGenerics} store, final int storeFieldId, final int fieldId) {
        return new ${type.name}WritableField() {
            @Override
            public ${type.arrayType} valueAt(final int row) {
                return store.get${type.name}(row, storeFieldId);
            }
            @Override
            public void setValueAt(final int row, final ${type.arrayType} value) {
                listener.fieldChanged(fieldId);
                store.set${type.name}(row, storeFieldId, value);
            }
        };
    }
</#list>
}
