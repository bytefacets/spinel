// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.tools.console;

import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.ui.Pager;
import org.fusesource.jansi.Ansi;

final class Control {
    private static final int TABLE_OFFSET = 3;
    // come back to make controllable
    private static final int PAGE = 0;
    private static final int PAGE_SIZE = 30;
    private final Ansi ansi;
    private final Presenter presenter;
    private final Pager pager;

    Control(final Presenter presenter, final Pager pager) {
        this.presenter = requireNonNull(presenter, "presenter");
        this.pager = requireNonNull(pager, "pager");
        this.ansi = presenter.ansi();
    }

    void setTitle(final String value) {
        ansi.append(String.format("\033]0;%s\007", value));
        presenter.update();
    }

    void emitClear() {
        ansi.eraseScreen();
        ansi.cursor(0, 0);
        presenter.update();
    }

    void repaint() {
        ansi.eraseScreen();
        ansi.cursor(0, 0);
        presenter.renderHeader();
        ansi.newline();
        presenter.renderHorizontalRule();
        ansi.newline();
        final int pageStart = PAGE * PAGE_SIZE;
        pager.rowsInRange(pageStart, PAGE_SIZE, this::repaintRow);
    }

    void repaintRow(final int row) {
        final int pagePosition = pager.rowPosition(row);
        final int pageStart = PAGE * PAGE_SIZE;
        final int pageEnd = pageStart + PAGE_SIZE;
        if (pagePosition >= pageStart && pagePosition < pageEnd) {
            final int screenRow = pagePosition + TABLE_OFFSET;
            ansi.cursor(screenRow, 0);
            presenter.renderRow(row);
            ansi.newline();
        }
    }

    void repaintRow(final int pagePosition, final int row) {
        ansi.cursor(pagePosition + TABLE_OFFSET, 0);
        presenter.renderRow(row);
        ansi.newline();
    }
}
