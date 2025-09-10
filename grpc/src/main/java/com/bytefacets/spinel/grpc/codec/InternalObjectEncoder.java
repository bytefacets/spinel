// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.codec;

interface InternalObjectEncoder {
    void encode(InternalBufferSupplier bufferSupplier, Object value);

    static InternalObjectEncoder userEncoder(final ObjectEncoder encoder) {
        return encoder::encode;
    }
}
