// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.nats.kv;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.KeyValueOperation;
import io.nats.client.api.KeyValueWatcher;
import io.netty.channel.EventLoop;
import java.time.Duration;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KvDataQueueTest {
    private static final Duration BUDGET = Duration.ofMillis(10);
    private static final long BUDGET_NANOS = BUDGET.toNanos();
    private @Mock EventLoop eventLoop;
    private @Mock KvDataQueue.Listener listener;
    private @Mock LongSupplier nanoSupplier;
    private @Mock KeyValueEntry entry;
    private KvDataQueue queue;
    private KeyValueWatcher watcher;

    @BeforeEach
    void setUp() {
        lenient().when(entry.getOperation()).thenReturn(KeyValueOperation.PUT);
        lenient().when(entry.getKey()).thenReturn("my-key");
        queue = new KvDataQueue(eventLoop, listener, BUDGET, nanoSupplier);
        watcher = queue.kvWatcher();
    }

    @Nested
    class SignalTests {
        @Test
        void shouldSignalDataArrival() {
            watcher.watch(entry);
            assertThat(queue.isSignalled(), equalTo(true));
            verify(eventLoop, times(1)).execute(any());
        }

        @Test
        void shouldSignalEndOfData() {
            watcher.endOfData();
            assertThat(queue.isSignalled(), equalTo(true));
            verify(eventLoop, times(1)).execute(any());
        }

        @Test
        void shouldResetSignalWhenRunning() {
            watcher.watch(entry);
            queue.run();
            assertThat(queue.isSignalled(), equalTo(false));
        }
    }

    @Nested
    class EntryTests {
        @Test
        void shouldCallListenerOnData() {
            watcher.watch(entry);
            queue.run();
            verify(listener, times(1)).update(entry);
        }

        @Test
        void shouldCallListenerDeleteOnPurge() {
            when(entry.getOperation()).thenReturn(KeyValueOperation.PURGE);
            watcher.watch(entry);
            queue.run();
            verify(listener, times(1)).delete("my-key");
        }

        @Test
        void shouldCallListenerDeleteOnDelete() {
            when(entry.getOperation()).thenReturn(KeyValueOperation.PURGE);
            watcher.watch(entry);
            queue.run();
            verify(listener, times(1)).delete("my-key");
        }
    }

    @Nested
    class EndOfDataTests {
        @Test
        void shouldCallListenerOnEndOfData() {
            watcher.endOfData();
            queue.run();
            verify(listener, times(1)).caughtUp();
        }
    }

    @Test
    void shouldLimitProcessingToBudgetedTime() {
        watcher.watch(entry);
        watcher.watch(entry);
        when(nanoSupplier.getAsLong()).thenReturn(0L, BUDGET_NANOS + 1);
        queue.run();
        verify(listener, times(1)).update(entry);
        assertThat(queue.free(), equalTo(1));
        assertThat(queue.pending(), equalTo(1));
    }

    @Test
    void shouldUseFreeList() {
        watcher.watch(entry);
        queue.run();
        verify(listener, times(1)).update(entry);

        final KeyValueEntry entry2 = mock(KeyValueEntry.class);
        when(entry2.getOperation()).thenReturn(KeyValueOperation.PUT);

        watcher.watch(entry2);
        queue.run();
        verify(listener, times(1)).update(entry2);

        assertThat(queue.free(), equalTo(1));
        assertThat(queue.pending(), equalTo(0));
    }
}
