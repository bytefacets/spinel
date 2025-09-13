// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.nats.kv;

import static com.bytefacets.spinel.grpc.receive.SchemaBuilder.schemaBuilder;
import static com.bytefacets.spinel.nats.kv.BucketUtil.DATA_PREFIX;
import static com.bytefacets.spinel.nats.kv.BucketUtil.SCHEMA_PREFIX;
import static com.bytefacets.spinel.printer.OutputLoggerBuilder.logger;
import static com.bytefacets.spinel.schema.ArrayFieldFactory.writableIntArrayField;
import static com.bytefacets.spinel.schema.FieldList.fieldList;
import static com.bytefacets.spinel.schema.MatrixStoreFieldFactory.matrixStoreFieldFactory;
import static com.bytefacets.spinel.schema.Schema.schema;
import static com.bytefacets.spinel.validation.Key.key;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.bytefacets.spinel.common.Connector;
import com.bytefacets.spinel.grpc.proto.FieldDefinition;
import com.bytefacets.spinel.grpc.proto.SchemaUpdate;
import com.bytefacets.spinel.schema.Field;
import com.bytefacets.spinel.schema.IntWritableField;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.TypeId;
import com.bytefacets.spinel.validation.RowData;
import com.bytefacets.spinel.validation.ValidationOperator;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.KeyValueOperation;
import io.nats.client.api.KeyValueWatcher;
import io.netty.channel.EventLoop;
import java.time.Duration;
import java.util.LinkedHashMap;
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
import org.slf4j.event.Level;

@ExtendWith(MockitoExtension.class)
class NatsKvSourceTest {
    private final ValidationOperator validator =
            new ValidationOperator(new String[] {"Key"}, "V1", "V2");
    private final BucketEncoder encoder = new BucketEncoder();
    private final IntWritableField keyField = writableIntArrayField(8, 0, i -> {});
    private final IntWritableField v1Field = writableIntArrayField(8, 1, i -> {});
    private final IntWritableField v2Field = writableIntArrayField(8, 2, i -> {});
    private @Mock KeyValue keyValue;
    private @Mock EventLoop eventLoop;
    private @Captor ArgumentCaptor<KeyValueWatcher> watcherCaptor;
    private @Captor ArgumentCaptor<Runnable> runnableCaptor;
    private KeyValueWatcher watcher;
    private SchemaUpdate schemaUpdate;
    private final RowData.RowDataTemplate template = RowData.template("V1", "V2");

    @BeforeEach
    void setUp() throws Exception {
        final SchemaRegistry schemaRegistry = new SchemaRegistry(keyValue);
        final NatsKvSource source =
                new NatsKvSource(
                        keyValue,
                        eventLoop,
                        schemaBuilder(matrixStoreFieldFactory(16, 16, i -> {})),
                        schemaRegistry,
                        Duration.ofMinutes(1),
                        16);
        verify(keyValue, times(1)).watchAll(watcherCaptor.capture());
        watcher = watcherCaptor.getValue();
        final Map<String, Field> fieldMap = new LinkedHashMap<>();
        fieldMap.put("Key", keyField);
        fieldMap.put("V1", v1Field);
        fieldMap.put("V2", v2Field);
        final Schema schema = schema("test", fieldList(fieldMap));
        schemaUpdate =
                SchemaUpdate.newBuilder()
                        .setName("test")
                        .addAllFields(List.of(field("Key"), field("V1"), field("V2")))
                        .build();
        encoder.setSchema(5, schema);
        Connector.connectInputToOutput(logger().logLevel(Level.INFO).build(), source);
        Connector.connectInputToOutput(validator, source);
    }

    @AfterEach
    void tearDown() {
        validator.assertNoActiveValidation();
    }

    @Nested
    class SchemaTests {
        @Test
        void shouldEmitSchemaWhenCaughtUp() {
            putSchema();
            watcher.endOfData();
            applyEvents();
            validator
                    .expect()
                    .schema(Map.of("Key", Integer.class, "V1", Integer.class, "V2", Integer.class))
                    .validate();
        }

        @Test
        void shouldNotEmitSchemaBeforeCaughtUp() {
            putSchema();
            applyEvents();
            validator.expect().validate();
        }
    }

    @Nested
    class AddTests {
        @Test
        void shouldAddRows() {
            initializeSchema();
            set(4, 10, 100, 1000);
            set(5, 20, 200, 2000);
            putEntry(4, "10");
            putEntry(5, "20");
            applyEvents();
            validator
                    .expect()
                    .added(key(10), template.rowData(100, 1000))
                    .added(key(20), template.rowData(200, 2000))
                    .validate();
        }

        @Test
        void shouldBufferRowsUntilCaughtUp() {
            set(4, 10, 100, 1000);
            set(5, 20, 200, 2000);
            putEntry(4, "10");
            putEntry(5, "20");
            putSchema();
            applyEvents();
            validator.expect().validate();

            watcher.endOfData();
            applyEvents();
            validator
                    .expect()
                    .added(key(10), template.rowData(100, 1000))
                    .added(key(20), template.rowData(200, 2000))
                    .validate();
        }
    }

    @Nested
    class ChangeTests {
        @Test
        void shouldChangeRows() {
            initializeSchema();
            set(4, 10, 100, 1000);
            set(5, 20, 200, 2000);
            putEntry(4, "10");
            putEntry(5, "20");
            applyEvents();
            validator.clearChanges();
            // add completed
            // when
            set(5, 20, 200, 3000);
            putEntry(5, "20");
            applyEvents();
            // then
            validator.expect().changed(key(20), template.rowData(null, 3000)).validate();
        }

        @Test
        void shouldBufferChangesUntilCaughtUp() {
            set(4, 10, 100, 1000);
            set(5, 20, 200, 2000);
            putEntry(4, "10");
            putEntry(5, "20");
            set(5, 20, 200, 3000);
            putEntry(5, "20");
            putSchema();
            applyEvents();
            validator.expect().validate();
            // when
            watcher.endOfData();
            applyEvents();
            // then
            validator
                    .expect()
                    .added(key(10), template.rowData(100, 1000))
                    .added(key(20), template.rowData(200, 3000))
                    .validate();
        }
    }

    @Nested
    class RemoveTests {
        @Test
        void shouldRemoveRows() {
            initializeSchema();
            set(4, 10, 100, 1000);
            set(5, 20, 200, 2000);
            putEntry(4, "10");
            putEntry(5, "20");
            applyEvents();
            validator.clearChanges();
            // add completed
            // when
            set(5, 20, 200, 3000);
            removeEntry("20", KeyValueOperation.DELETE);
            applyEvents();
            // then
            validator.expect().removed(key(20)).validate();
        }

        @Test
        void shouldBufferRemovesUntilCaughtUp() {
            set(4, 10, 100, 1000);
            set(5, 20, 200, 2000);
            putEntry(4, "10");
            putEntry(5, "20");
            set(5, 20, 200, 3000);
            removeEntry("20", KeyValueOperation.DELETE);
            putSchema();
            applyEvents();
            validator.expect().validate();
            // when
            watcher.endOfData();
            applyEvents();
            // then -- no "20"
            validator.expect().added(key(10), template.rowData(100, 1000)).validate();
        }
    }

    private void initializeSchema() {
        putSchema();
        watcher.endOfData();
        applyEvents();
        validator.clearChanges();
    }

    private FieldDefinition field(final String name) {
        return FieldDefinition.newBuilder().setName(name).setTypeId(TypeId.Int).build();
    }

    void putSchema() {
        final KeyValueEntry entry = mock(KeyValueEntry.class);
        lenient().when(entry.getOperation()).thenReturn(KeyValueOperation.PUT);
        lenient().when(entry.getValue()).thenReturn(schemaUpdate.toByteArray());
        lenient().when(entry.getKey()).thenReturn(SCHEMA_PREFIX + 5);
        watcher.watch(entry);
    }

    void putEntry(final int row, final String key) {
        final KeyValueEntry entry = mock(KeyValueEntry.class);
        lenient().when(entry.getOperation()).thenReturn(KeyValueOperation.PUT);
        lenient().when(entry.getValue()).thenReturn(encoder.encode(row));
        lenient().when(entry.getKey()).thenReturn(DATA_PREFIX + key);
        watcher.watch(entry);
    }

    void removeEntry(final String key, final KeyValueOperation op) {
        final KeyValueEntry entry = mock(KeyValueEntry.class);
        lenient().when(entry.getOperation()).thenReturn(op);
        lenient().when(entry.getKey()).thenReturn(DATA_PREFIX + key);
        watcher.watch(entry);
    }

    private void set(final int row, final int key, final int v1, final int v2) {
        keyField.setValueAt(row, key);
        v1Field.setValueAt(row, v1);
        v2Field.setValueAt(row, v2);
    }

    private void applyEvents() {
        verify(eventLoop, times(1)).execute(runnableCaptor.capture());
        runnableCaptor.getValue().run();
        reset(eventLoop);
    }
}
