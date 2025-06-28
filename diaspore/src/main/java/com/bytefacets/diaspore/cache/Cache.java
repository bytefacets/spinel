// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.cache;

import static com.bytefacets.diaspore.exception.FieldNotFoundException.fieldNotFound;
import static com.bytefacets.diaspore.exception.SchemaNotBoundException.schemaNotBound;
import static com.bytefacets.diaspore.schema.MatrixStoreFieldFactory.matrixStoreFieldFactory;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.collections.hash.IntGenericIndexedMap;
import com.bytefacets.collections.hash.StringGenericIndexedMap;
import com.bytefacets.diaspore.schema.ChangedFieldSet;
import com.bytefacets.diaspore.schema.Field;
import com.bytefacets.diaspore.schema.FieldCopier;
import com.bytefacets.diaspore.schema.FieldCopierFactory;
import com.bytefacets.diaspore.schema.FieldDescriptor;
import com.bytefacets.diaspore.schema.FieldList;
import com.bytefacets.diaspore.schema.FieldResolver;
import com.bytefacets.diaspore.schema.MatrixStoreFieldFactory;
import com.bytefacets.diaspore.schema.Metadata;
import com.bytefacets.diaspore.schema.Schema;
import com.bytefacets.diaspore.schema.SchemaField;
import com.bytefacets.diaspore.schema.WritableField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

public final class Cache {
    private final MatrixStoreFieldFactory matrixStoreFactory;
    private final IntGenericIndexedMap<FieldCopier> inboundIdToCopier;
    private final StringGenericIndexedMap<SchemaField> fieldMap;
    private final Resolver resolver = new Resolver();
    private final Copier copier = new Copier();

    Cache(final Set<String> fields, final int initialSize, final int chunkSize) {
        this.matrixStoreFactory = matrixStoreFieldFactory(initialSize, chunkSize, i -> {});
        this.inboundIdToCopier = new IntGenericIndexedMap<>(Math.max(1, fields.size()), 1d);
        this.fieldMap = new StringGenericIndexedMap<>(Math.max(1, fields.size()), 1d);
        requireNonNull(fields, "fields").forEach(fieldMap::add);
    }

    public FieldResolver resolver() {
        return resolver;
    }

    public void unbind() {
        resolver.isBoundToSchema = false;
        inboundIdToCopier.clear();
        // We don't have a good way of resetting all internal data of the writable field/store,
        // so the simplest thing is to rebuild the fields when the cache is bound
        for (int i = 0, len = fieldMap.size(); i < len; i++) {
            fieldMap.putValueAt(i, null);
        }
    }

    public void bind(final Schema inSchema) {
        final Map<Byte, List<FieldDescriptor>> typeMap = buildTypeMap(inSchema);
        final FieldList cacheFields = matrixStoreFactory.createFieldList(fieldMap, typeMap);
        buildAndMapCache(inSchema, cacheFields);
        resolver.isBoundToSchema = true;
    }

    private Map<Byte, List<FieldDescriptor>> buildTypeMap(final Schema inSchema) {
        final Map<Byte, List<FieldDescriptor>> typeMap = new HashMap<>(fieldMap.size(), 1f);
        for (int id = 0, len = fieldMap.size(); id < len; id++) {
            final String fieldName = fieldMap.getKeyAt(id);
            final SchemaField inField = inSchema.maybeField(fieldName);
            if (inField == null) {
                throw fieldNotFound(fieldName, "cached field", inSchema.name());
            }
            final var descriptor = new FieldDescriptor(inField.typeId(), fieldName, Metadata.EMPTY);
            typeMap.computeIfAbsent(inField.typeId(), k -> new ArrayList<>()).add(descriptor);
        }
        return typeMap;
    }

    private void buildAndMapCache(final Schema inSchema, final FieldList cacheFields) {
        for (int id = 0, len = fieldMap.size(); id < len; id++) {
            final String fieldName = fieldMap.getKeyAt(id);
            final SchemaField inField = inSchema.field(fieldName);
            final WritableField cacheField = (WritableField) cacheFields.fieldAt(id).field();
            inboundIdToCopier.put(
                    inField.fieldId(), FieldCopierFactory.fieldCopier(inField.field(), cacheField));
        }
    }

    public void updateSelected(final IntIterable rows, final ChangedFieldSet changed) {
        copier.applyChanges(rows, changed);
    }

    public void updateAll(final IntIterable rows) {
        rows.forEach(this::fireAllCopiers);
    }

    private void fireAllCopiers(final int row) {
        for (int copierEntry = 0, size = inboundIdToCopier.size();
                copierEntry < size;
                copierEntry++) {
            inboundIdToCopier.getValueAt(copierEntry).copy(row);
        }
    }

    /** Copier separated out to avoid object allocation as lambda */
    private final class Copier {
        private int row;
        private ChangedFieldSet changed;

        private void applyChanges(final IntIterable rows, final ChangedFieldSet changed) {
            this.changed = changed;
            rows.forEach(this::copyChangedRowFields);
        }

        private void copyChangedRowFields(final int row) {
            this.row = row;
            changed.forEach(this::copyField);
        }

        private void copyField(final int inFieldId) {
            final int entry = inboundIdToCopier.lookupEntry(inFieldId);
            if (entry != -1) {
                inboundIdToCopier.getValueAt(entry).copy(row);
            }
        }
    }

    private final class Resolver implements FieldResolver {
        private boolean isBoundToSchema;

        @Nullable
        @Override
        public Field findField(final String name) {
            if (!isBoundToSchema) {
                throw schemaNotBound("trying to access field " + name + " in cache");
            }
            final int entry = fieldMap.lookupEntry(name);
            return entry != -1 ? fieldMap.getValueAt(entry).field() : null;
        }
    }
}
