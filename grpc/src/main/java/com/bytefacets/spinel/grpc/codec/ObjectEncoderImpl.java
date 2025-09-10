// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.codec;

import com.bytefacets.spinel.schema.TypeId;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;

public final class ObjectEncoderImpl {
    private static final int HEADER_SIZE = 2;
    private static final byte[] EMPTY_ARRAY = new byte[0];
    private final BufferSupplierImpl bufferSupplier = new BufferSupplierImpl();
    private ByteBuffer buffer = ByteBuffer.allocate(64);

    public static ObjectEncoderImpl encoder() {
        return new ObjectEncoderImpl();
    }

    private ObjectEncoderImpl() {}

    public ByteString encode(final Object value) {
        final InternalObjectEncoder encoder = ObjectEncoderRegistry.lookup(value);
        if (encoder != null) {
            buffer.clear();
            encoder.encode(bufferSupplier, value);
            return ByteString.copyFrom(buffer.flip());
        } else {
            return ByteString.EMPTY;
        }
    }

    public byte[] encodeToArray(final Object value) {
        final InternalObjectEncoder encoder = ObjectEncoderRegistry.lookup(value);
        if (encoder != null) {
            buffer.clear();
            encoder.encode(bufferSupplier, value);
            final byte[] copy = new byte[buffer.position()];
            buffer.flip().get(copy);
            return copy;
        } else {
            return EMPTY_ARRAY;
        }
    }

    private final class BufferSupplierImpl implements InternalBufferSupplier {
        @Override
        public ByteBuffer beginSystemType(final byte systemTypeId, final int lengthOfValue) {
            ensureSize(lengthOfValue);
            buffer.put(systemTypeId);
            buffer.put((byte) 0);
            return buffer;
        }

        @Override
        public ByteBuffer beginUserType(final byte userTypeId, final int lengthOfValue) {
            ensureSize(lengthOfValue);
            buffer.put(TypeId.Generic);
            buffer.put(userTypeId);
            return buffer;
        }

        private void ensureSize(final int lengthOfValue) {
            final int additionalPlusHeader = lengthOfValue + HEADER_SIZE;
            final int minSize = buffer.position() + additionalPlusHeader;
            if (minSize >= buffer.capacity()) {
                final int nextSize = Math.max(minSize, buffer.capacity() * 2);
                final ByteBuffer newBuffer = ByteBuffer.allocate(nextSize);
                newBuffer.put(buffer.flip());
                buffer = newBuffer;
            }
        }
    }
}
