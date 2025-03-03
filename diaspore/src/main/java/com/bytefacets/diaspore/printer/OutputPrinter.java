// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.printer;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.diaspore.TransformInput;
import com.bytefacets.diaspore.schema.ChangedFieldSet;
import com.bytefacets.diaspore.schema.Schema;
import com.bytefacets.diaspore.schema.TypeId;
import java.io.PrintStream;

public final class OutputPrinter {
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

    public TransformInput input() {
        return input;
    }

    private static final class Input implements TransformInput {
        private final PrintStream out;
        private Schema schema;

        private Input(final PrintStream out) {
            this.out = requireNonNull(out, "out");
        }

        @Override
        public void schemaUpdated(final Schema schema) {
            this.schema = schema;
            if (schema != null) {
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
            schema.forEachField(
                    field -> out.printf("[%s=%s]", field.name(), field.objectValueAt(row)));
            out.println();
        }

        private void printChange(final int row, final ChangedFieldSet changed) {
            out.printf("CHG r%s: ", padRowId(row));
            changed.forEach(
                    fieldId -> {
                        final var field = schema.fieldAt(fieldId);
                        out.printf("[%s=%s]", field.name(), field.objectValueAt(row));
                    });
            out.println();
        }

        private void printRemove(final int row) {
            out.printf("REM r%s%n", padRowId(row));
        }
    }
}
