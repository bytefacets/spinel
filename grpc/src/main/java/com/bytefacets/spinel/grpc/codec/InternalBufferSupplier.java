// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.codec;

import java.nio.ByteBuffer;

interface InternalBufferSupplier extends BufferSupplier {
    ByteBuffer beginSystemType(byte systemTypeId, int lengthOfValue);
}
