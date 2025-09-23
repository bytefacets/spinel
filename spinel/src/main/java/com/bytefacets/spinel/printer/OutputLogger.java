// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.printer;

import static com.bytefacets.spinel.common.DelegatedRowProvider.delegatedRowProvider;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.spinel.TransformInput;
import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.common.OutputManager;
import com.bytefacets.spinel.schema.ChangedFieldSet;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.SchemaField;
import com.bytefacets.spinel.schema.TypeId;
import com.bytefacets.spinel.transform.InputProvider;
import com.bytefacets.spinel.transform.OutputProvider;
import jakarta.annotation.Nullable;
import java.util.BitSet;
import java.util.LinkedHashSet;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.event.Level;

public final class OutputLogger implements InputProvider, OutputProvider {
    private final Input input;

    OutputLogger(
            final Logger logger,
            final BiConsumer<Logger, String> logMethod,
            final Level logLevel,
            final LinkedHashSet<String> forcedFields,
            final RendererRegistry rendererRegistry) {
        this.input = new Input(logger, forcedFields, logMethod, logLevel, rendererRegistry);
    }

    @Override
    public TransformInput input() {
        return input;
    }

    @Override
    public TransformOutput output() {
        return input.outputManager.output();
    }

    public boolean enabled() {
        return input.enabled;
    }

    public void enabled(final boolean enabled) {
        this.input.enabled = enabled;
    }

    private static final class Input implements TransformInput {
        private final Logger logger;
        private final BiConsumer<Logger, String> logMethod;
        private final Level logLevel;
        private final StringBuilder sb = new StringBuilder(128);
        private final OutputManager outputManager;
        private final RendererRegistry rendererRegistry;
        private final LinkedHashSet<String> forcedFields;
        private final BitSet forceFieldIdSet = new BitSet();
        private SchemaField[] orderedFields;
        private ValueRenderer[] renderers;
        private TransformOutput source;
        private boolean enabled = true;
        private long eventId;
        private Schema schema;

        private Input(
                final Logger logger,
                final LinkedHashSet<String> forcedFields,
                final BiConsumer<Logger, String> logMethod,
                final Level logLevel,
                final RendererRegistry rendererRegistry) {
            this.logger = requireNonNull(logger, "logger");
            this.logMethod = requireNonNull(logMethod, "logMethod");
            this.logLevel = requireNonNull(logLevel, "logLevel");
            this.rendererRegistry = requireNonNull(rendererRegistry, "rendererRegistry");
            this.outputManager = OutputManager.outputManager(delegatedRowProvider(() -> source));
            this.forcedFields = forcedFields;
        }

        @Override
        public void setSource(@Nullable final TransformOutput output) {
            this.source = output;
        }

        private boolean doLog() {
            return enabled && logger.isEnabledForLevel(logLevel);
        }

        private void logIt(final String message) {
            logMethod.accept(logger, message);
        }

        @Override
        public void schemaUpdated(final Schema schema) {
            this.schema = schema;
            if (schema != null) {
                initializeFields(schema);
            } else {
                orderedFields = null;
                renderers = null;
                forceFieldIdSet.clear();
            }
            if (doLog()) {
                if (schema != null) {
                    logSchema(eventId);
                } else {
                    renderers = null;
                    logIt(String.format("e%-10d SCH null", eventId));
                }
            }
            outputManager.updateSchema(schema);
        }

        private void logSchema(final long id) {
            sb.setLength(0);
            sb.append(String.format("e%-10d SCH %s: %d fields ", id, schema.name(), schema.size()));
            for (final SchemaField field : orderedFields) {
                sb.append(
                        String.format(
                                "[%d,%s,%s]",
                                field.fieldId(), field.name(), TypeId.toTypeName(field.typeId())));
            }
            logIt(sb.toString());
        }

        @Override
        public void rowsAdded(final IntIterable rows) {
            if (doLog()) {
                rows.forEach(this::printAdd);
            }
            outputManager.notifyAdds(rows);
            eventId++;
        }

        @Override
        public void rowsChanged(final IntIterable rows, final ChangedFieldSet changedFields) {
            if (doLog()) {
                rows.forEach(row -> printChange(row, changedFields));
            }
            outputManager.notifyChanges(rows, changedFields);
            eventId++;
        }

        @Override
        public void rowsRemoved(final IntIterable rows) {
            if (doLog()) {
                rows.forEach(this::printRemove);
            }
            outputManager.notifyRemoves(rows);
            eventId++;
        }

        private void printAdd(final int row) {
            sb.setLength(0);
            sb.append(String.format("e%-10d ADD r%-6d: ", eventId, row));
            for (final SchemaField field : orderedFields) {
                sb.append('[').append(field.name()).append('=');
                renderers[field.fieldId()].render(sb, row);
                sb.append(']');
            }
            logIt(sb.toString());
        }

        private void printChange(final int row, final ChangedFieldSet changed) {
            sb.setLength(0);
            sb.append(String.format("e%-10d CHG r%-6d: ", eventId, row));
            final int forcedSize = forcedFields.size();
            for (int i = 0; i < forcedSize; i++) {
                final var field = orderedFields[i];
                sb.append('[').append(field.name()).append('=');
                renderers[field.fieldId()].render(sb, row);
                sb.append(']');
            }
            changed.forEach(
                    fieldId -> {
                        if (!forceFieldIdSet.get(fieldId)) {
                            final var field = schema.fieldAt(fieldId);
                            sb.append('[').append(field.name()).append('=');
                            renderers[field.fieldId()].render(sb, row);
                            sb.append(']');
                        }
                    });
            logIt(sb.toString());
        }

        private void printRemove(final int row) {
            sb.setLength(0);
            sb.append(String.format("e%-10d REM r%-6d:", eventId, row));
            logIt(sb.toString());
        }

        private void initializeFields(final Schema schema) {
            orderedFields = new SchemaField[schema.size()];
            renderers = rendererRegistry.renderers(schema);
            int nextIx = 0;
            for (String fieldName : forcedFields) {
                final SchemaField field = schema.field(fieldName);
                orderedFields[nextIx++] = field;
                forceFieldIdSet.set(field.fieldId());
            }
            for (int i = 0, len = schema.size(); i < len; i++) {
                final SchemaField field = schema.fieldAt(i);
                if (!forcedFields.contains(field.name())) {
                    orderedFields[nextIx++] = field;
                }
            }
        }
    }
}
