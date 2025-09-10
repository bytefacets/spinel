// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.codec;

/**
 * @see ObjectEncoderRegistry#register(Class, ObjectEncoder)
 * @see ObjectDecoder#decode(java.nio.ByteBuffer)
 */
public interface ObjectEncoder {
    /**
     * Extension point for implementation to encode the `value` into the ByteBuffer returned by the
     * BufferSupplier. In your implementation, call `beginUserType` with your chosen id and the
     * length of the value you're about to encode; it will give you a ByteBuffer into which you can
     * write the value. In the ObjectDecoder, you'll just read the value out of the ByteBuffer using
     * the remaining bytes.
     *
     * @param bufferSupplier given a type and length, it will return you an appropriately sized and
     *     positioned ByteBuffer
     * @param value the value to encode
     */
    void encode(BufferSupplier bufferSupplier, Object value);
}
