// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.nats.kv;

import static com.bytefacets.spinel.common.StateChangeSet.stateChangeSet;
import static com.bytefacets.spinel.schema.FieldDescriptor.intField;
import static com.bytefacets.spinel.schema.FieldDescriptor.stringField;
import static com.bytefacets.spinel.schema.MatrixStoreFieldFactory.matrixStoreFieldFactory;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.bytefacets.collections.types.IntType;
import com.bytefacets.spinel.common.Connector;
import com.bytefacets.spinel.schema.FieldBitSet;
import com.bytefacets.spinel.schema.IntWritableField;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.StringWritableField;
import com.bytefacets.spinel.schema.TypeId;
import com.bytefacets.spinel.validation.Key;
import com.bytefacets.spinel.validation.RowData;
import com.bytefacets.spinel.validation.ValidationOperator;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.KeyValueOperation;
import io.nats.client.api.KeyValueWatcher;
import io.netty.channel.EventLoop;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class NatsKvAdapterTest {
    private final ValidationOperator validator =
            new ValidationOperator(new String[] {"Key"}, "V1", "V2");
    private @Mock KeyValue keyValue;
    private @Mock EventLoop eventLoop;
    private @Mock(strictness = Mock.Strictness.LENIENT) KvUpdateHandler handler;
    private @Captor ArgumentCaptor<KeyValueWatcher> watcherCaptor;
    private @Captor ArgumentCaptor<Runnable> runnableCaptor;
    private @Captor ArgumentCaptor<KeyValueEntry> entryCaptor;
    private final FieldBitSet fieldBitSet = FieldBitSet.fieldBitSet();
    private final RowData.RowDataTemplate template = RowData.template("V1", "V2");
    private NatsKvAdapter adapter;
    private KeyValueWatcher watcher;
    private Schema schema;

    @BeforeEach
    void setUp() throws Exception {
        final var fieldMap =
                Map.of(
                        TypeId.Int,
                        List.of(intField("V1"), intField("V2")),
                        TypeId.String,
                        List.of(stringField("Key")));
        schema =
                Schema.schema(
                        "test-schema",
                        matrixStoreFieldFactory(4, 4, fieldBitSet::fieldChanged)
                                .createFieldList(fieldMap));
        setUpHandler();
        adapter =
                new NatsKvAdapter(
                        keyValue,
                        eventLoop,
                        schema,
                        stateChangeSet(fieldBitSet),
                        handler,
                        Duration.ofMinutes(1),
                        4);
        adapter.open();
        verify(keyValue, times(1)).watchAll(watcherCaptor.capture());
        watcher = watcherCaptor.getValue();
        Connector.connectInputToOutput(validator, adapter);
        validator.clearChanges();
    }

    @Test
    void shouldNotifyHandlerWhenCaughtUp() {
        watcher.endOfData();
        applyEvents();
        verify(handler, times(1)).caughtUp();
    }

    @Nested
    class AddTests {
        @Test
        void shouldAddWhenNewKeys() {
            putEntry("k1", 10, 30);
            putEntry("k2", 20, 50);
            applyEvents();
            validator
                    .expect()
                    .added(Key.key("k1"), template.rowData(10, 30))
                    .added(Key.key("k2"), template.rowData(20, 50))
                    .validate();
        }
    }

    @Nested
    class ChangeTests {
        @BeforeEach
        void setUp() {
            putEntry("k1", 10, 30);
            putEntry("k2", 20, 50);
            applyEvents();
            validator.clearChanges();
        }

        @Test
        void shouldChangeWhenKeysSeenAgain() {
            putEntry("k1", 11, 31);
            putEntry("k2", 21, 51);
            applyEvents();
            validator
                    .expect()
                    .changed(Key.key("k1"), template.rowData(11, 31))
                    .changed(Key.key("k2"), template.rowData(21, 51))
                    .validate();
        }
    }

    @Nested
    class RemoveTests {
        @BeforeEach
        void setUp() {
            putEntry("k1", 10, 30);
            putEntry("k2", 20, 50);
            applyEvents();
            validator.clearChanges();
        }

        @Test
        void shouldAddWhenNewDeleteOperation() {
            removeEntry("k2", KeyValueOperation.DELETE);
            applyEvents();
            validator.expect().removed(Key.key("k2")).validate();
        }

        @Test
        void shouldAddWhenNewPurgeOperation() {
            removeEntry("k2", KeyValueOperation.PURGE);
            applyEvents();
            validator.expect().removed(Key.key("k2")).validate();
        }

        @Test
        void shouldCallbackHandlerWhenUnknownKey() {
            removeEntry("unknown", KeyValueOperation.DELETE);
            verify(handler, times(1)).unknownDeletedEntry(entryCaptor.capture());
            assertThat(entryCaptor.getValue().getKey(), equalTo("unknown"));
        }
    }

    @AfterEach
    void tearDown() {
        validator.validateNoChanges();
    }

    void putEntry(final String key, final int v1, final int v2) {
        final KeyValueEntry entry = mock(KeyValueEntry.class);
        final byte[] value = new byte[8];
        IntType.writeLE(value, 0, v1);
        IntType.writeLE(value, 4, v2);
        lenient().when(entry.getOperation()).thenReturn(KeyValueOperation.PUT);
        lenient().when(entry.getValue()).thenReturn(value);
        lenient().when(entry.getKey()).thenReturn(key);
        watcher.watch(entry);
    }

    void removeEntry(final String key, final KeyValueOperation op) {
        final KeyValueEntry entry = mock(KeyValueEntry.class);
        lenient().when(entry.getOperation()).thenReturn(op);
        lenient().when(entry.getKey()).thenReturn(key);
        watcher.watch(entry);
    }

    private void applyEvents() {
        verify(eventLoop, times(1)).execute(runnableCaptor.capture());
        runnableCaptor.getValue().run();
        reset(eventLoop);
    }

    private void setUpHandler() {
        doAnswer(
                        inv -> {
                            final int row = inv.getArgument(0, Integer.class);
                            final KeyValueEntry entry = inv.getArgument(1, KeyValueEntry.class);
                            setKey(row, entry.getKey());
                            setV1(row, entry.getValue());
                            setV2(row, entry.getValue());
                            return null;
                        })
                .when(handler)
                .updated(anyInt(), any());
    }

    private void setKey(final int row, final String key) {
        (((StringWritableField) schema.field("Key").field())).setValueAt(row, key);
    }

    private void setV1(final int row, final byte[] value) {
        final int intValue = IntType.readLE(value, 0);
        (((IntWritableField) schema.field("V1").field())).setValueAt(row, intValue);
    }

    private void setV2(final int row, final byte[] value) {
        final int intValue = IntType.readLE(value, 4);
        (((IntWritableField) schema.field("V2").field())).setValueAt(row, intValue);
    }
}
