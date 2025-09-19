// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.tools.console;

import static java.util.Objects.requireNonNull;

import org.fusesource.jansi.Ansi;

final class Control {
    private final Ansi ansi;
    private final Presenter presenter;
    private final RowMapping rowMapping;

    Control(final Presenter presenter, final RowMapping rowMapping) {
        this.presenter = requireNonNull(presenter, "presenter");
        this.rowMapping = requireNonNull(rowMapping, "rowMapping");
        this.ansi = presenter.ansi();
    }

    void emitClear() {
        ansi.eraseScreen();
        ansi.cursor(0, 0);
        presenter.update();
    }

    void clearRow(final int screenRow) {
        ansi.cursor(screenRow, 0);
        ansi.eraseLine();
    }

    void repaint() {
        ansi.eraseScreen();
        ansi.cursor(0, 0);
        presenter.renderHeader();
        ansi.newline();
        rowMapping.forEach(this::repaintRow);
    }

    void repaintRow(final int row) {
        final int screenRow = rowMapping.screenRow(row);
        ansi.cursor(screenRow, 0);
        presenter.renderRow(row);
        ansi.newline();
    }
}
