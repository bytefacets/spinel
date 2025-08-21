// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.examples;

import com.bytefacets.collections.functional.IntConsumer;
import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.schema.Schema;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoop;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Util {

    private Util() {}

    public static ManagedChannel clientChannel(final String target) {
        return ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .enableRetry()
                .keepAliveTime(5, TimeUnit.MINUTES)
                .keepAliveTimeout(20, TimeUnit.SECONDS)
                .build();
    }

    public static EventLoop newEventLoop(final String name) {
        return new DefaultEventLoop(
                r -> {
                    return new Thread(r, name);
                });
    }

    @SuppressWarnings("DataFlowIssue")
    public static Runnable dumper(final String loggerName, final TransformOutput output) {
        final Logger clientLog = LoggerFactory.getLogger(loggerName);
        final StringBuilder sb = new StringBuilder();
        final AtomicInteger count = new AtomicInteger();
        final IntConsumer printRow =
                row -> {
                    sb.setLength(0);
                    final Schema schema = output.schema();
                    schema.forEachField(
                            schemaField -> {
                                final var value = schemaField.field().objectValueAt(row);
                                sb.append(String.format("[%s=%s]", schemaField.name(), value));
                            });
                    clientLog.info("Dumping Row[{}]: {}", row, sb);
                    count.getAndIncrement();
                };
        return () -> {
            count.set(0);
            final Schema schema = output.schema();
            if (schema == null) {
                clientLog.info("Schema not available");
                return; // no schema from the server yet
            }
            output.rowProvider().forEach(printRow);
            if (count.get() == 0) {
                clientLog.info("Schema available, but no rows");
            }
        };
    }
}
