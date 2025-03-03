// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.comms.receive;

public interface ChangeDecoder<T> {
    void accept(T message);
}
