// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.prototype;

import static com.bytefacets.spinel.common.BitSetRowProvider.bitSetRowProvider;
import static com.bytefacets.spinel.common.OutputManager.outputManager;
import static com.bytefacets.spinel.schema.FieldMapping.fieldMapping;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.collections.vector.IntVector;
import com.bytefacets.spinel.TransformInput;
import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.common.BitSetRowProvider;
import com.bytefacets.spinel.common.OutputManager;
import com.bytefacets.spinel.schema.ChangedFieldSet;
import com.bytefacets.spinel.schema.Field;
import com.bytefacets.spinel.schema.FieldBitSet;
import com.bytefacets.spinel.schema.FieldMapping;
import com.bytefacets.spinel.schema.FieldResolver;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.SchemaBindable;
import com.bytefacets.spinel.transform.InputProvider;
import com.bytefacets.spinel.transform.OutputProvider;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A Prototype operator is a trade-off: declare a schema locally which is kind of bad, in exchange
 * for schema stability from upstream. One would typically connect this to the output of something
 * like GrpcSource which is doing the heavy work of consuming the subscription. But if you want a
 * consumer, like a JavaFX UI or something, to have a stable schema, you can use the prototype as a
 * shim.
 *
 * <p>Another positive with the Prototype is that it can perform some level of type-casting using
 * {@link com.bytefacets.spinel.schema.Cast}, so if the server sends a short, but you said the field
 * was an int locally, the Prototype will manage that discrepancy for you.
 */
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
            activeRows.clear();
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
