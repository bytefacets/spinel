// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.tools.console;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.arrays.BoolArray;
import com.bytefacets.collections.arrays.IntArray;
import com.bytefacets.spinel.printer.ValueRenderer;
import com.bytefacets.spinel.schema.TypeId;
import java.util.function.Consumer;
import org.fusesource.jansi.Ansi;

/** Owns the instance of Ansi and is responsible for dealing with the text, but no control. */
final class Presenter {
    private final StringBuilder renderSb = new StringBuilder();
    private final StringBuilder ansiSb = new StringBuilder();
    private final Ansi ansi = new Ansi(ansiSb);
    private final Consumer<String> emitter;

    private int[] columnWidths = new int[8];
    private boolean[] isNumeric = new boolean[8];
    private ValueRenderer[] renderers;
    private String[] headers;
    private int schemaWidth;

    Presenter(final Consumer<String> emitter) {
        this.emitter = requireNonNull(emitter, "emitter");
    }

    Ansi ansi() {
        return ansi;
    }

    void update() {
        emitter.accept(ansi.toString());
        ansiSb.setLength(0);
    }

    void reset() {
        renderers = null;
    }

    void initialize(final int schemaWidth) {
        this.schemaWidth = schemaWidth;
        columnWidths = IntArray.ensureSize(columnWidths, schemaWidth);
        isNumeric = BoolArray.ensureSize(isNumeric, schemaWidth);
        renderers = new ValueRenderer[schemaWidth];
        headers = new String[schemaWidth];
    }

    void initializeField(
            final String name, final int fieldId, final byte typeId, final ValueRenderer renderer) {
        columnWidths[fieldId] = width(name);
        renderers[fieldId] = requireNonNull(renderer, "renderer");
        headers[fieldId] = name;
        isNumeric[fieldId] = Number.class.isAssignableFrom(TypeId.toClass(typeId));
        renderValue(ansi, fieldId, name);
    }

    boolean calculateColumnWidths(final int row) {
        boolean repaint = false;
        for (int fieldId = 0; fieldId < schemaWidth; fieldId++) {
            if (calculateColumnWidth(row, fieldId)) {
                repaint = true;
            }
        }
        return repaint;
    }

    boolean calculateColumnWidth(final int row, final int fieldId) {
        final int width = width(loadRenderValue(fieldId, row));
        if (width > columnWidths[fieldId]) {
            columnWidths[fieldId] = width;
            return true;
        } else {
            return false;
        }
    }

    void renderHeader() {
        for (int fieldId = 0; fieldId < schemaWidth; fieldId++) {
            renderValue(ansi, fieldId, headers[fieldId]);
        }
    }

    void renderRow(final int row) {
        for (int fieldId = 0; fieldId < schemaWidth; fieldId++) {
            renderValue(ansi, fieldId, loadRenderValue(fieldId, row));
        }
    }

    private void renderValue(final Ansi ansi, final int fieldId, final CharSequence value) {
        final String fmtString = " %" + columnWidths[fieldId] + "s ";
        ansi.append(String.format(fmtString, value));
    }

    CharSequence loadRenderValue(final int fieldId, final int row) {
        renderSb.setLength(0);
        renderers[fieldId].render(renderSb, row);
        return renderSb;
    }

    // VisibleForTesting
    static int width(final CharSequence value) {
        if (value != null) {
            return value.length() + 2;
        } else {
            return "null".length() + 2;
        }
    }
}
