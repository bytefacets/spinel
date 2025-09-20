// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.tools.console;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElseGet;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.spinel.TransformInput;
import com.bytefacets.spinel.printer.RendererRegistry;
import com.bytefacets.spinel.schema.ChangedFieldSet;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.transform.InputProvider;
import jakarta.annotation.Nullable;

public final class ConsoleRenderer implements InputProvider {
    private final RendererRegistry rendererRegistry = RendererRegistry.rendererRegistry();
    private final Input input;
    private final String title;

    ConsoleRenderer(final String title) {
        this(title, new Presenter(System.out::println), new RowMapping(), null);
    }

    // VisibleForTesting
    ConsoleRenderer(
            final String title,
            final Presenter presenter,
            final RowMapping mapping,
            final Control control) {
        this.title = requireNonNull(title, "title");
        input =
                new Input(
                        presenter,
                        mapping,
                        requireNonNullElseGet(control, () -> new Control(presenter, mapping)));
    }

    @Override
    public TransformInput input() {
        return input;
    }

    private final class Input implements TransformInput {
        private final Presenter presenter;
        private final RowMapping mapping;
        private final Control control;
        private boolean repaint = true;

        Input(final Presenter presenter, final RowMapping mapping, final Control control) {
            this.presenter = requireNonNull(presenter, "presenter");
            this.mapping = requireNonNull(mapping, "mapping");
            this.control = requireNonNull(control, "control");
            control.setTitle(title);
        }

        @Override
        public void schemaUpdated(@Nullable final Schema schema) {
            if (schema != null) {
                presenter.initialize(schema.size());
                schema.forEachField(
                        sField -> {
                            presenter.initializeField(
                                    sField.name(),
                                    sField.fieldId(),
                                    sField.typeId(),
                                    rendererRegistry.renderer(sField));
                        });
                control.emitClear();
                presenter.renderHeader();
                presenter.update();
            } else {
                presenter.reset();
                mapping.reset();
                control.emitClear();
            }
        }

        @Override
        public void rowsAdded(final IntIterable rows) {
            rows.forEach(this::mapAndCalculateWidths);
            if (repaint) {
                repaint = false;
                control.repaint();
            } else {
                rows.forEach(control::repaintRow);
            }
            presenter.update();
        }

        @Override
        public void rowsChanged(final IntIterable rows, final ChangedFieldSet changedFields) {
            changedFields.forEach(fieldId -> rows.forEach(row -> recalculateWidth(row, fieldId)));
            if (repaint) {
                repaint = false;
                control.repaint();
            } else {
                rows.forEach(control::repaintRow);
            }
            presenter.update();
        }

        @Override
        public void rowsRemoved(final IntIterable rows) {
            rows.forEach(this::clearRow);
            presenter.update();
        }

        /** Called on added rows, to recalculate widths and assign a screen row */
        private void mapAndCalculateWidths(final int row) {
            if (presenter.calculateColumnWidths(row)) {
                repaint = true;
            }
            mapping.mapRow(row);
        }

        /** Called on updated rows to recalculate widths */
        private void recalculateWidth(final int row, final int fieldId) {
            if (presenter.calculateColumnWidth(row, fieldId)) {
                repaint = true;
            }
        }

        /** Called on removed rows */
        private void clearRow(final int row) {
            control.clearRow(mapping.freeRow(row));
        }
    }
}
