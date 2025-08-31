// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.spring;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.spinel.TransformInput;
import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.comms.send.ChangeEncoder;
import com.bytefacets.spinel.schema.ChangedFieldSet;
import com.bytefacets.spinel.schema.Schema;
import io.netty.channel.EventLoop;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

final class FluxAdapter<T> {
    private static final Logger log = LoggerFactory.getLogger(FluxAdapter.class);
    private final Input input = new Input();
    private final TransformOutput source;
    private final ChangeEncoder<T> encoder;
    private ReactivePublisher publisher;

    FluxAdapter(final ChangeEncoder<T> encoder, final TransformOutput source) {
        this.encoder = requireNonNull(encoder, "encoder");
        this.source = requireNonNull(source, "source");
    }

    private void fluxReady() {
        source.attachInput(input);
    }

    private class Input implements TransformInput {
        @Override
        public void schemaUpdated(@Nullable final Schema schema) {
            if (schema != null) {
                publisher.enqueue(encoder.encodeSchema(schema));
            }
        }

        @Override
        public void rowsAdded(final IntIterable rows) {
            publisher.enqueue(encoder.encodeAdd(rows));
        }

        @Override
        public void rowsChanged(final IntIterable rows, final ChangedFieldSet changedFields) {
            publisher.enqueue(encoder.encodeChange(rows, changedFields));
        }

        @Override
        public void rowsRemoved(final IntIterable rows) {
            publisher.enqueue(encoder.encodeRemove(rows));
        }
    }

    Flux<T> flux(final EventLoop dataLoop) {
        return Flux.create(
                sink -> {
                    publisher = new ReactivePublisher(sink, dataLoop);
                    Thread.ofVirtual().start(publisher);
                    dataLoop.execute(this::fluxReady);
                });
    }

    private class ReactivePublisher implements Runnable {
        private final FluxSink<T> sink;
        private final LinkedBlockingDeque<T> queue = new LinkedBlockingDeque<>();
        private final EventLoop dataLoop;
        private final AtomicBoolean connected = new AtomicBoolean(true);

        ReactivePublisher(final FluxSink<T> sink, final EventLoop dataLoop) {
            this.sink = requireNonNull(sink, "sink");
            this.dataLoop = dataLoop;
            this.sink.onCancel(this::disconnect);
            this.sink.onDispose(this::disconnect);
        }

        private void enqueue(final T data) {
            if (log.isTraceEnabled()) {
                log.trace("Queueing data...{}", data);
            }
            queue.offer(data);
        }

        @Override
        public void run() {
            while (connected.get()) {
                try {
                    final T data = queue.pollFirst(10, TimeUnit.SECONDS);
                    if (data != null) {
                        log.trace("Sending data...{}", data);
                        sink.next(data);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            log.info("Exiting FluxAdapter Thread");
        }

        private void disconnect() {
            if (connected.compareAndSet(true, false)) {
                log.info("Disconnecting FluxAdapter");
                dataLoop.execute(() -> source.detachInput(input));
            }
        }
    }
}
