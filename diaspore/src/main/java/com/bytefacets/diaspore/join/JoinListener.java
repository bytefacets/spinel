// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.join;

interface JoinListener {
    void joinAdded(int outRow);

    void joinUpdated(int outRow, boolean leftReplaced, boolean rightReplaced);

    void joinRemoved(int outRow);
}
