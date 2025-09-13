// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.nats.kv;

import static com.bytefacets.spinel.nats.FieldSequenceNatsSubjectBuilder.fieldSequenceNatsSubjectBuilder;
import static com.bytefacets.spinel.nats.kv.BucketUtil.DATA_PREFIX;
import static com.bytefacets.spinel.table.IntIndexedStructTableBuilder.intIndexedStructTable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.bytefacets.collections.store.StringChunkStore;
import com.bytefacets.collections.types.IntType;
import com.bytefacets.spinel.common.Connector;
import com.bytefacets.spinel.grpc.proto.SchemaUpdate;
import com.bytefacets.spinel.table.IntIndexedStructTable;
import io.nats.client.KeyValue;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class NatsKvSinkTest {
    private final StringChunkStore keyStore = new StringChunkStore(16, 16);
    private final IntIndexedStructTable<Data> source = intIndexedStructTable(Data.class).build();
    private final Data facade = source.createFacade();
    private final FieldReader fieldReader = new FieldReader();
    private @Mock KeyValue kv;
    private @Mock SchemaRegistry schemaRegistry;
    private @Captor ArgumentCaptor<SchemaUpdate> schemaCaptor;
    private @Captor ArgumentCaptor<String> keyCaptor;
    private @Captor ArgumentCaptor<byte[]> valueCaptor;
    private NatsKvSink sink;

    @BeforeEach
    void setUp() throws Exception {
        lenient().when(schemaRegistry.register(any())).thenReturn(1);
        sink =
                new NatsKvSink(
                        kv,
                        fieldSequenceNatsSubjectBuilder(List.of("Value1")),
                        keyStore,
                        schemaRegistry);
        Connector.connectInputToOutput(sink.input(), source);
    }

    @Test
    void shouldPublishSchema() throws Exception {
        verify(schemaRegistry, times(1)).register(schemaCaptor.capture());
    }

    @Nested
    class AddTests {
        @Test
        void shouldPublishAdds() throws Exception {
            add(5, "k1", 6);
            add(7, "k2", 8);
            source.fireChanges();
            verify(kv, times(2)).put(keyCaptor.capture(), valueCaptor.capture());
            assertThat(keyCaptor.getAllValues(), contains(DATA_PREFIX + "k1", DATA_PREFIX + "k2"));
            validate(valueCaptor.getAllValues().get(0), 5, "k1", 6);
            validate(valueCaptor.getAllValues().get(1), 7, "k2", 8);
        }

        @Test
        void shouldRecordKeysInKeyStore() {
            final int r1 = add(5, "k1", 6);
            final int r2 = add(7, "k2", 8);
            source.fireChanges();
            assertThat(keyStore.getString(r1), equalTo("k1"));
            assertThat(keyStore.getString(r2), equalTo("k2"));
        }
    }

    @Nested
    class ChangeTests {
        int row1;
        int row2;

        @BeforeEach
        void setUp() {
            row1 = add(5, "k1", 6);
            row2 = add(7, "k2", 8);
            source.fireChanges();
            reset(kv);
        }

        @Test
        void shouldPublishChanges() throws Exception {
            source.beginChange(5, facade).setValue2(9);
            source.endChange();
            source.beginChange(7, facade).setValue2(10);
            source.endChange();
            source.fireChanges();
            verify(kv, times(2)).put(keyCaptor.capture(), valueCaptor.capture());
            assertThat(keyCaptor.getAllValues(), contains(DATA_PREFIX + "k1", DATA_PREFIX + "k2"));
            validate(valueCaptor.getAllValues().get(0), 5, "k1", 9);
            validate(valueCaptor.getAllValues().get(1), 7, "k2", 10);
        }

        @Test
        void shouldRebuildSubjectWhenKeyFieldChanges() throws Exception {
            source.beginChange(7, facade).setValue1("up2");
            source.endChange();
            source.fireChanges();
            verify(kv, times(1)).delete(DATA_PREFIX + "k2");
            verify(kv, times(1)).put(keyCaptor.capture(), valueCaptor.capture());
            assertThat(keyCaptor.getValue(), equalTo(DATA_PREFIX + "up2"));
            validate(valueCaptor.getValue(), 7, "up2", 8);
            assertThat(keyStore.getString(row1), equalTo("k1"));
            assertThat(keyStore.getString(row2), equalTo("up2"));
        }

        @Test
        void shouldJustUpdateWhenKeyFieldIsMarkedButDoesNotChange() throws Exception {
            source.beginChange(7, facade).setValue1("k2");
            source.endChange();
            source.fireChanges();
            verify(kv, never()).delete(any());
            verify(kv, times(1)).put(keyCaptor.capture(), valueCaptor.capture());
            assertThat(keyCaptor.getValue(), equalTo(DATA_PREFIX + "k2"));
            validate(valueCaptor.getValue(), 7, "k2", 8);
            assertThat(keyStore.getString(row1), equalTo("k1"));
            assertThat(keyStore.getString(row2), equalTo("k2"));
        }
    }

    @Nested
    class RemoveTests {
        private int row1;
        private int row2;

        @BeforeEach
        void setUp() {
            row1 = add(5, "k1", 6);
            row2 = add(7, "k2", 8);
            source.fireChanges();
            reset(kv);
        }

        @Test
        void shouldPublishRemoves() throws Exception {
            source.remove(5);
            source.fireChanges();
            verify(kv, times(1)).delete(keyCaptor.capture());
            assertThat(keyCaptor.getAllValues(), contains(DATA_PREFIX + "k1"));
        }

        @Test
        void shouldRemoveKeysFromStore() {
            source.remove(7);
            source.fireChanges();
            assertThat(keyStore.getString(row1), equalTo("k1"));
            assertThat(keyStore.getString(row2), nullValue());
        }
    }

    private int add(final int key, final String v1, final int v2) {
        source.beginAdd(key, facade).setValue1(v1).setValue2(v2);
        source.endAdd();
        return source.lookupKeyRow(key);
    }

    private void validate(final byte[] data, final int key, final String v1, final int v2) {
        assertThat(IntType.readLE(data, 0), equalTo(1)); // schemaId
        fieldReader.set(4, data); // skip schemaId
        for (int i = 0, len = source.schema().size(); i < len; i++) {
            switch (source.schema().fieldAt(i).name()) {
                case "Key" -> assertThat(fieldReader.readToInt(), equalTo(key));
                case "Value1" -> assertThat(fieldReader.readToString(), equalTo(v1));
                case "Value2" -> assertThat(fieldReader.readToInt(), equalTo(v2));
                default -> throw new RuntimeException("unknown field " + i);
            }
        }
    }

    interface Data {
        int getKey();

        String getValue1();

        Data setValue1(String value);

        int getValue2();

        Data setValue2(int value);
    }
}
