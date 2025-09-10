// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.codec;

import java.nio.ByteBuffer;

/**
 * @see com.bytefacets.spinel.grpc.codec.ObjectEncoder
 */
public interface BufferSupplier {
    /**
     * For implementers of ObjectEncoder, this will return you a positioned and sized ByteBuffer
     *
     * @param userTypeId your type identifier
     * @param lengthOfValue the length you need for encoding your value
     * @return a ByteBuffer in which the encoding should be written
     */
    ByteBuffer beginUserType(byte userTypeId, int lengthOfValue);
}
