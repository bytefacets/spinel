// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.receive;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntConsumer;
import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.common.BitSetRowProvider;
import com.bytefacets.spinel.common.OutputManager;
import com.bytefacets.spinel.common.StateChange;
import com.bytefacets.spinel.comms.receive.ChangeDecoder;
import com.bytefacets.spinel.grpc.proto.DataUpdate;
import com.bytefacets.spinel.grpc.proto.Response;
import com.bytefacets.spinel.grpc.proto.ResponseType;
import com.bytefacets.spinel.grpc.proto.SchemaUpdate;
import com.bytefacets.spinel.grpc.proto.SubscriptionResponse;
import com.bytefacets.spinel.schema.TypeId;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class GrpcDecoder implements ChangeDecoder<SubscriptionResponse> {
    private static final Logger log = LoggerFactory.getLogger(GrpcDecoder.class);
    private final BitSet changedFieldIds = new BitSet();
    private final StateChange stateChange = StateChange.stateChange(changedFieldIds);
    private final BitSet activeRows = new BitSet();
    private final OutputManager outputManager =
            OutputManager.outputManager(BitSetRowProvider.bitSetRowProvider(activeRows));
    private final List<TypeReader> readers = new ArrayList<>(TypeId.Max);
    private final SchemaBuilder schemaBuilder;

    static GrpcDecoder grpcDecoder(final SchemaBuilder schemaBuilder) {
        return new GrpcDecoder(schemaBuilder);
    }

    private GrpcDecoder(final SchemaBuilder schemaBuilder) {
        this.schemaBuilder = requireNonNull(schemaBuilder, "schemaBuilder");
        readers.add(new BoolReader());
        readers.add(new ByteReader());
        readers.add(new Int32Reader());
        readers.add(new Int64Reader());
        readers.add(new StringReader());
        readers.add(new ByteReader());
        readers.add(new DoubleReader());
        readers.add(new FloatReader());
        readers.add(new GenericReader());
    }

    @Override
    public void accept(final SubscriptionResponse message) {
        switch (message.getResponseType()) {
            case RESPONSE_TYPE_SCHEMA -> applySchema(message.getSchema());
            case RESPONSE_TYPE_ADD -> applyAdd(message.getData());
            case RESPONSE_TYPE_CHG -> applyChange(message.getData());
            case RESPONSE_TYPE_REM -> applyRemove(message.getData());
            case RESPONSE_TYPE_MESSAGE -> notifyOwnerWithMessage(message.getResponse());
            default -> logUnknownResponseType(message);
        }
    }

    private void notifyOwnerWithMessage(final Response response) {
        if (response.getError()) {
            log.warn("Received error message: {}", response.getMessage());
        } else {
            log.info("Received message: {}", response.getMessage());
        }
    }

    TransformOutput output() {
        return outputManager.output();
    }

    private void logUnknownResponseType(final SubscriptionResponse message) {
        log.warn(
                "Server might be on newer version; received unknown SubscriptionResponse type: {}",
                message.getResponseType());
    }

    private void applyAdd(final DataUpdate data) {
        iterateRowsWithActiveChange(data, stateChange::addRow, activeRows::set);
        applyData(data, ResponseType.RESPONSE_TYPE_ADD);
    }

    private void applyChange(final DataUpdate data) {
        iterateChangedRows(data, stateChange::changeRow);
        applyData(data, ResponseType.RESPONSE_TYPE_CHG);
    }

    private void applyRemove(final DataUpdate data) {
        iterateRowsWithActiveChange(data, stateChange::removeRow, activeRows::clear);
        stateChange.fire(outputManager, activeRows::clear);
    }

    private void applyData(final DataUpdate data, final ResponseType op) {
        readers.forEach(reader -> reader.read(data, op));
        stateChange.fire(outputManager, activeRows::clear);
    }

    private void iterateRowsWithActiveChange(
            final DataUpdate msg, final IntConsumer consumer, final IntConsumer activeChange) {
        final int rowCt = msg.getRowsCount();
        for (int i = 0; i < rowCt; i++) {
            final int row = msg.getRows(i);
            consumer.accept(row);
            activeChange.accept(row);
        }
    }

    private void iterateChangedRows(final DataUpdate msg, final IntConsumer consumer) {
        final int rowCt = msg.getRowsCount();
        for (int i = 0; i < rowCt; i++) {
            final int row = msg.getRows(i);
            consumer.accept(row);
        }
    }

    private void applySchema(final SchemaUpdate schemaMessage) {
        final int fieldCt = schemaMessage.getFieldsCount();
        if (fieldCt != 0) {
            final var schema = schemaBuilder.createSchema(schemaMessage);
            readers.forEach(reader -> reader.setContext(schema.fields(), changedFieldIds));
            outputManager.updateSchema(schema);
        } else {
            activeRows.clear();
            outputManager.updateSchema(null);
        }
    }
}
