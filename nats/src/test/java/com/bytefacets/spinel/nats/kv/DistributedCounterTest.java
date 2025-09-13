// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.nats.kv;

import static com.bytefacets.spinel.nats.kv.DistributedCounter.counter;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bytefacets.collections.types.IntType;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.api.Error;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.KeyValueOperation;
import io.nats.client.api.KeyValueWatcher;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DistributedCounterTest {
    private @Mock KeyValue kv;
    private @Captor ArgumentCaptor<KeyValueWatcher> watcherCaptor;
    private DistributedCounter counter;
    private KeyValueWatcher watcher;

    @BeforeEach
    void setUp() throws JetStreamApiException, IOException, InterruptedException {
        counter = counter(kv, "my-counter");
        verify(kv, times(1)).watch(eq("my-counter"), watcherCaptor.capture());
        watcher = watcherCaptor.getValue();
    }

    @Test
    void shouldCountWhileUnableToUpdateKey() throws Exception {
        final var updates = new LinkedList<>(List.of(mockEntry(10, 1), mockEntry(11, 2)));
        // race condition of an attempt to update right before receiving an update
        doAnswer(
                        inv -> {
                            if (!updates.isEmpty()) {
                                watcher.watch(updates.removeFirst());
                                throw badRevision();
                            }
                            return 300L;
                        })
                .when(kv)
                .update(any(), any(byte[].class), anyLong());
        assertThat(counter.increment(), equalTo(12));
        assertThat(counter.revision(), equalTo(300L));
    }

    @Test
    void shouldUpdateLatestFromWatcher() {
        watcher.watch(mockEntry(5, 789L));
        assertThat(counter.revision(), equalTo(789L));
        assertThat(counter.currentValue(), equalTo(5));
    }

    @Test
    void shouldResetOnPurge() {
        final KeyValueEntry entry = mockEntry(5, 789L);
        when(entry.getOperation()).thenReturn(KeyValueOperation.PURGE);
        watcher.watch(entry);
        assertThat(counter.revision(), equalTo(789L));
        assertThat(counter.currentValue(), equalTo(0));
    }

    @Test
    void shouldResetOnDelete() {
        final KeyValueEntry entry = mockEntry(5, 789L);
        when(entry.getOperation()).thenReturn(KeyValueOperation.DELETE);
        watcher.watch(entry);
        assertThat(counter.revision(), equalTo(789L));
        assertThat(counter.currentValue(), equalTo(0));
    }

    @Test
    void shouldNotUpdateWhenRevisionIsLessThanCurrent() {
        watcher.watch(mockEntry(5, 789L));
        watcher.watch(mockEntry(9, 555L));
        assertThat(counter.revision(), equalTo(789L));
        assertThat(counter.currentValue(), equalTo(5));
    }

    private KeyValueEntry mockEntry(final long revision) {
        final KeyValueEntry entry = mock(KeyValueEntry.class);
        when(entry.getRevision()).thenReturn(revision);
        return entry;
    }

    private KeyValueEntry mockEntry(final int value, final long revision) {
        final KeyValueEntry entry = mock(KeyValueEntry.class);
        lenient().when(entry.getOperation()).thenReturn(KeyValueOperation.PUT);
        when(entry.getRevision()).thenReturn(revision);
        final byte[] data = new byte[4];
        IntType.writeLE(data, 0, value);
        lenient().when(entry.getValue()).thenReturn(data);
        return entry;
    }

    private JetStreamApiException badRevision() {
        final Error e = mock(Error.class);
        when(e.getApiErrorCode()).thenReturn(10071);
        return new JetStreamApiException(e);
    }
}
