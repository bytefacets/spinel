// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.receive;

final class ExponentialBackOff {
    private static final long MAX_DELAY = 30_000;
    private static final long INITIAL_DELAY = 1000;
    private static final double JITTER = 0.2;

    private long nextDelayMillis = INITIAL_DELAY;

    long nextDelayMillis() {
        final double jitterFactor = 1 + (Math.random() * 2 - 1) * JITTER;
        final long delay = (long) Math.min(nextDelayMillis * jitterFactor, MAX_DELAY);
        nextDelayMillis = Math.min(nextDelayMillis << 1, MAX_DELAY);
        return delay;
    }

    void reset() {
        nextDelayMillis = INITIAL_DELAY;
    }
}
