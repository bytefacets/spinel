// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.codec;

import java.nio.ByteBuffer;

/**
 * @see ObjectEncoder
 * @see ObjectDecoderRegistry#register(byte, ObjectDecoder)
 */
public interface ObjectDecoder {
    /**
     * Object decoding extension point
     *
     * @param buffer the buffer with the encoded value positioned for reading
     * @return the decoded object
     */
    Object decode(ByteBuffer buffer);
}
