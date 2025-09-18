// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc;

import static com.bytefacets.spinel.schema.FieldDescriptor.intField;
import static com.bytefacets.spinel.table.IntIndexedTableBuilder.intIndexedTable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.bytefacets.spinel.comms.receive.ChangeDecoder;
import com.bytefacets.spinel.grpc.proto.Response;
import com.bytefacets.spinel.grpc.proto.ResponseType;
import com.bytefacets.spinel.grpc.proto.SubscriptionResponse;
import com.bytefacets.spinel.grpc.receive.ReceivePackageAccess;
import com.bytefacets.spinel.grpc.send.GrpcSink;
import com.bytefacets.spinel.grpc.send.SendPackageAccess;
import com.bytefacets.spinel.printer.OutputLoggerBuilder;
import com.bytefacets.spinel.schema.Metadata;
import com.bytefacets.spinel.table.IntIndexedTable;
import com.bytefacets.spinel.testing.IntTableHandle;
import com.bytefacets.spinel.validation.Key;
import com.bytefacets.spinel.validation.RowData;
import com.bytefacets.spinel.validation.ValidationOperator;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

public final class CodecTest {
    private static final boolean print = true;
    private final SendPackageAccess sendPkg = new SendPackageAccess();
    private final ValidationOperator validation =
            new ValidationOperator(new String[] {"Id"}, "Value1", "Value2");
    private final RowData.RowDataTemplate template = RowData.template("Value1", "Value2");
    private IntIndexedTable table;
    private IntTableHandle tableHandle;
    private GrpcSink sendingAdapter;
    private ReceivePackageAccess.DecoderAccess receiver;

    @BeforeEach
    void setUp() {
        table =
                intIndexedTable("table")
                        .addFields(intField("Value1"), intField("Value2"))
                        .keyFieldName("Id")
                        .build();
        tableHandle = IntTableHandle.intTableHandle("Id", table);
    }

    @AfterEach
    void tearDown() {
        validation.assertNoActiveValidation();
    }

    @Nested
    class RowChangeTests {
        @BeforeEach
        void setUp() {
            wire();
            addRows(1, 2);
            table.fireChanges();
        }

        @Test
        void shouldSendAdd() {
            validation
                    .expect()
                    .added(key(1), template.rowData(10, 100))
                    .added(key(2), template.rowData(20, 200))
                    .validate();
        }

        @Test
        void shouldSendChange() {
            validation.clearChanges();
            // when
            tableHandle.change(1, 11, null);
            tableHandle.change(2, 21, null);
            table.fireChanges();
            // then
            validation
                    .expect()
                    .changed(key(1), template.rowData(11, null))
                    .changed(key(2), template.rowData(21, null))
                    .validate();
        }

        @Test
        void shouldSendRemove() {
            validation.clearChanges();
            // when
            table.remove(1);
            table.remove(2);
            table.fireChanges();
            // then
            validation.expect().removed(key(1)).removed(key(2)).validate();
        }
    }

    @Nested
    class SchemaTests {
        @BeforeEach
        void setUp() {
            wire();
        }

        @Test
        void shouldSendSchema() {
            validation.expect().schema(expectedSchema()).validate();
        }

        @Test
        void shouldSendNullSchema() {
            wire();
            addRows(1, 2);
            table.fireChanges();
            validation.clearChanges();
            // when
            table.output().detachInput(sendingAdapter.input());
            // then
            validation.expect().nullSchema().validate();
        }
    }

    @Nested
    class MetaDataTests {
        private void setUp(final Metadata v1Md, final Metadata v2Md) {
            table =
                    intIndexedTable("table")
                            .addFields(intField("Value1", v1Md), intField("Value2", v2Md))
                            .keyFieldName("Id")
                            .build();
            tableHandle = IntTableHandle.intTableHandle("Id", table);
            wire();
        }

        @Test
        void shouldTransmitFieldMetadata() {
            final Metadata v1Md = Metadata.metadata(Set.of("v1"), Map.of("a1", 1));
            final Metadata v2Md = Metadata.metadata(Set.of("v2"), Map.of("a2", 2L));
            setUp(v1Md, v2Md);
            validation
                    .expect()
                    .schema(expectedSchema())
                    .metadata(Map.of("Value1", v1Md, "Value2", v2Md))
                    .validate();
        }

        @Test
        void shouldNotTransmitAttributeWhenTypesNotRegistered() {
            final Metadata v1Md = Metadata.metadata(Map.of("a1", 1, "transient", new Object()));
            final Metadata v2Md = Metadata.metadata(Map.of("a2", 2L));
            setUp(v1Md, v2Md);
            validation
                    .expect()
                    .schema(expectedSchema())
                    .metadata(Map.of("Value1", Metadata.metadata(Map.of("a1", 1)), "Value2", v2Md))
                    .validate();
        }
    }

    @Test
    void shouldNotThrowWhenUnknownResponseType() {
        wire();
        final var response = mock(SubscriptionResponse.class);
        when(response.getResponseType()).thenReturn(ResponseType.UNRECOGNIZED);
        when(response.getResponse())
                .thenReturn(
                        Response.newBuilder()
                                .setSuccess(true)
                                .setMessage("this is a test")
                                .build());
        receiver.accept(response);
    }

    // test support

    private void addRows(final int... salt) {
        for (int salt1 : salt) {
            tableHandle.add(salt1, salt1 * 10, salt1 * 100);
        }
    }

    private void wire() {
        // client ---------
        receiver = ReceivePackageAccess.decoder();
        if (print) {
            receiver.output()
                    .attachInput(OutputLoggerBuilder.logger().logLevel(Level.INFO).build().input());
        }
        // receiver goes to the validator
        receiver.output().attachInput(validation.input());

        // server ---------
        // wire the sender directly to the receiver
        sendingAdapter = sendPkg.sink(5, () -> 0, new MockNetwork(receiver));
        // table goes to the sender
        table.output().attachInput(sendingAdapter.input());
    }

    private static final class MockNetwork implements StreamObserver<SubscriptionResponse> {
        private final ChangeDecoder<SubscriptionResponse> decoder;

        private MockNetwork(final ChangeDecoder<SubscriptionResponse> decoder) {
            this.decoder = decoder;
        }

        @Override
        public void onNext(final SubscriptionResponse subscriptionResponse) {
            decoder.accept(subscriptionResponse);
        }

        @Override
        public void onError(final Throwable throwable) {}

        @Override
        public void onCompleted() {}
    }

    private Key key(final int key) {
        return new Key(List.of(key));
    }

    private Map<String, Class<?>> expectedSchema() {
        return Map.of("Id", Integer.class, "Value1", Integer.class, "Value2", Integer.class);
    }
}
