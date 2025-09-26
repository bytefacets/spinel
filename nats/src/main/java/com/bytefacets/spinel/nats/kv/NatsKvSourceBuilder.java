// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.nats.kv;

import com.bytefacets.spinel.grpc.receive.SchemaBuilder;
import com.bytefacets.spinel.schema.MatrixStoreFieldFactory;
import io.nats.client.JetStreamApiException;
import io.netty.channel.EventLoop;
import java.io.IOException;
import java.time.Duration;

/**
 * @see NatsKvSource
 * @see NatsKvSink
 */
public final class NatsKvSourceBuilder {
    private int initialSize = 128;
    private int chunkSize = 128;
    private EventLoop eventLoop;

    private NatsKvSourceBuilder() {}

    public static NatsKvSourceBuilder natsKvSource() {
        return new NatsKvSourceBuilder();
    }

    public NatsKvSourceBuilder initialSize(final int initialSize) {
        this.initialSize = initialSize;
        return this;
    }

    public NatsKvSourceBuilder chunkSize(final int chunkSize) {
        this.chunkSize = chunkSize;
        return this;
    }

    public NatsKvSourceBuilder eventLoop(final EventLoop eventLoop) {
        this.eventLoop = eventLoop;
        return this;
    }

    public NatsKvSource build() throws JetStreamApiException, IOException, InterruptedException {
        return new NatsKvSource(
                eventLoop,
                SchemaBuilder.schemaBuilder(
                        MatrixStoreFieldFactory.matrixStoreFieldFactory(
                                initialSize, chunkSize, i -> {})),
                new SchemaRegistry(),
                Duration.ofMillis(200),
                initialSize);
    }
}
