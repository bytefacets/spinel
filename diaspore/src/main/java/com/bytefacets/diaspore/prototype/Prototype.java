// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.prototype;

import static com.bytefacets.diaspore.common.BitSetRowProvider.bitSetRowProvider;
import static com.bytefacets.diaspore.common.OutputManager.outputManager;
import static com.bytefacets.diaspore.schema.FieldMapping.fieldMapping;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.collections.vector.IntVector;
import com.bytefacets.diaspore.TransformInput;
import com.bytefacets.diaspore.TransformOutput;
import com.bytefacets.diaspore.common.BitSetRowProvider;
import com.bytefacets.diaspore.common.OutputManager;
import com.bytefacets.diaspore.schema.ChangedFieldSet;
import com.bytefacets.diaspore.schema.Field;
import com.bytefacets.diaspore.schema.FieldBitSet;
import com.bytefacets.diaspore.schema.FieldMapping;
import com.bytefacets.diaspore.schema.FieldResolver;
import com.bytefacets.diaspore.schema.Schema;
import com.bytefacets.diaspore.schema.SchemaBindable;
import com.bytefacets.diaspore.transform.InputProvider;
import com.bytefacets.diaspore.transform.OutputProvider;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import javax.annotation.Nullable;

public final class Prototype implements InputProvider, OutputProvider {
    private final Input input;
    private final OutputManager outputManager;
    private final TransformOutput output;
    private final SchemaChangeManager provider;
    private final BitSet activeRows = new BitSet();
    private final BitSetRowProvider rowProvider = bitSetRowProvider(activeRows);

    Prototype(final PrototypeSchemaBuilder schemaBuilder, final int deletedRowBatchSize) {
        this.provider = new SchemaChangeManager(schemaBuilder.size());
        this.outputManager = outputManager(rowProvider);
        this.output = outputManager.output();
        final var outSchema = requireNonNull(schemaBuilder, "schemaBuilder").buildSchema(provider);
        outputManager.updateSchema(outSchema);
        input = new Input(deletedRowBatchSize);
    }

    @Override
    public TransformInput input() {
        return input;
    }

    @Override
    public TransformOutput output() {
        return output;
    }

    private final class SchemaChangeManager
            implements PrototypeFieldFactory.SchemaProvider, FieldResolver {
        private final List<SchemaBindable> bindables;
        private FieldMapping.Builder fieldMappingBuilder;
        private Schema inSchema;

        private SchemaChangeManager(final int size) {
            bindables = new ArrayList<>(size);
        }

        public void update(final Schema inSchema) {
            this.inSchema = inSchema;
            if (inSchema == null) {
                bindables.forEach(SchemaBindable::unbindSchema);
            } else {
                fieldMappingBuilder = fieldMapping(bindables.size());
                bindables.forEach(b -> b.bindToSchema(this));
                input.fieldMapping = fieldMappingBuilder.build();
            }
        }

        @Override
        public void registerForSchema(final SchemaBindable bindable) {
            bindables.add(bindable);
        }

        @Override
        public @Nullable Field findField(final String name) {
            final var schemaField = inSchema.maybeField(name);
            if (schemaField != null) {
                final int outFieldId = outputManager.schema().field(name).fieldId();
                final int inFieldId = schemaField.fieldId();
                fieldMappingBuilder.mapInboundToOutbound(inFieldId, outFieldId);
                return schemaField.field();
            }
            return null;
        }
    }

    private final class Input implements TransformInput {
        private final IntVector deletedRowBatch;
        private final int deletedRowBatchSize;
        private FieldMapping fieldMapping;
        private final FieldBitSet outFieldIds = FieldBitSet.fieldBitSet();

        private Input(final int deletedRowBatchSize) {
            this.deletedRowBatchSize = deletedRowBatchSize;
            deletedRowBatch = new IntVector(deletedRowBatchSize);
        }

        @Override
        public void schemaUpdated(@Nullable final Schema schema) {
            final boolean deleteAll = provider.inSchema != null && schema == null;
            provider.update(schema);
            if (deleteAll) {
                sendDeletesForActiveRows();
            }
        }

        @Override
        public void rowsAdded(final IntIterable rows) {
            rows.forEach(activeRows::set);
            outputManager.notifyAdds(rows);
        }

        @Override
        public void rowsChanged(final IntIterable rows, final ChangedFieldSet changedFields) {
            outFieldIds.clear();
            fieldMapping.translateInboundChangeSet(changedFields, outFieldIds::fieldChanged);
            if (outFieldIds.size() > 0) {
                outputManager.notifyChanges(rows, outFieldIds);
            }
        }

        @Override
        public void rowsRemoved(final IntIterable rows) {
            rows.forEach(activeRows::clear);
            outputManager.notifyRemoves(rows);
        }

        private void sendDeletesForActiveRows() {
            rowProvider.forEach(
                    row -> {
                        deletedRowBatch.append(row);
                        if (deletedRowBatch.size() == deletedRowBatchSize) {
                            fireDeletedRows();
                        }
                    });
            if (!deletedRowBatch.isEmpty()) {
                fireDeletedRows();
            }
        }

        private void fireDeletedRows() {
            outputManager.notifyRemoves(deletedRowBatch);
            deletedRowBatch.clear();
        }
    }
}
