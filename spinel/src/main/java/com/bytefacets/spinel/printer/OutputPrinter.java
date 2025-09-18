// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.printer;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.spinel.TransformInput;
import com.bytefacets.spinel.schema.ChangedFieldSet;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.SchemaField;
import com.bytefacets.spinel.schema.TypeId;
import com.bytefacets.spinel.transform.InputProvider;
import java.io.PrintStream;

public final class OutputPrinter implements InputProvider {
    private static final int ROW_ID_WIDTH = 5;
    private final TransformInput input;

    public static OutputPrinter printer(final PrintStream out) {
        return new OutputPrinter(out);
    }

    public static OutputPrinter printer() {
        return new OutputPrinter(System.out);
    }

    private OutputPrinter(final PrintStream out) {
        this.input = new Input(out);
    }

    @Override
    public TransformInput input() {
        return input;
    }

    private static final class Input implements TransformInput {
        private final RendererRegistry rendererRegistry = RendererRegistry.rendererRegistry();
        private final PrintStream out;
        private final StringBuilder sb = new StringBuilder();
        private ValueRenderer[] renderers;
        private Schema schema;

        private Input(final PrintStream out) {
            this.out = requireNonNull(out, "out");
        }

        @Override
        public void schemaUpdated(final Schema schema) {
            this.schema = schema;
            if (schema != null) {
                renderers = rendererRegistry.renderers(schema);
                out.printf("SCH %s: %d fields ", schema.name(), schema.size());
                schema.forEachField(
                        field ->
                                out.printf(
                                        "[%d,%s,%s]",
                                        field.fieldId(),
                                        field.name(),
                                        TypeId.toTypeName(field.typeId())));
                out.println();
            } else {
                out.println("SCH null");
                renderers = null;
            }
        }

        @Override
        public void rowsAdded(final IntIterable rows) {
            rows.forEach(this::printAdd);
        }

        @Override
        public void rowsChanged(final IntIterable rows, final ChangedFieldSet changedFields) {
            rows.forEach(row -> printChange(row, changedFields));
        }

        @Override
        public void rowsRemoved(final IntIterable rows) {
            rows.forEach(this::printRemove);
        }

        private String padRowId(final int row) {
            final String sRow = Integer.toString(row);
            return "_".repeat(ROW_ID_WIDTH - sRow.length()) + sRow;
        }

        private void printAdd(final int row) {
            out.printf("ADD r%s: ", padRowId(row));
            schema.forEachField(field -> out.print(render(field, row)));
            out.println();
        }

        private void printChange(final int row, final ChangedFieldSet changed) {
            out.printf("CHG r%s: ", padRowId(row));
            changed.forEach(fieldId -> out.print(render(schema.fieldAt(fieldId), row)));
            out.println();
        }

        private void printRemove(final int row) {
            out.printf("REM r%s%n", padRowId(row));
        }

        private StringBuilder render(final SchemaField field, final int row) {
            sb.setLength(0);
            final var render = renderers[field.fieldId()];
            render.render(sb.append('[').append(field.name()).append('='), row);
            sb.append(']');
            return sb;
        }
    }
}
