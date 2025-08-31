// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.send;

import com.bytefacets.collections.arrays.ByteArray;
import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.collections.vector.IntVector;
import com.bytefacets.spinel.comms.send.ChangeEncoder;
import com.bytefacets.spinel.grpc.codec.ObjectEncoderImpl;
import com.bytefacets.spinel.grpc.proto.BoolData;
import com.bytefacets.spinel.grpc.proto.ByteData;
import com.bytefacets.spinel.grpc.proto.DataUpdate;
import com.bytefacets.spinel.grpc.proto.DoubleData;
import com.bytefacets.spinel.grpc.proto.FieldDefinition;
import com.bytefacets.spinel.grpc.proto.FloatData;
import com.bytefacets.spinel.grpc.proto.GenericData;
import com.bytefacets.spinel.grpc.proto.Int32Data;
import com.bytefacets.spinel.grpc.proto.Int64Data;
import com.bytefacets.spinel.grpc.proto.Metadata;
import com.bytefacets.spinel.grpc.proto.ResponseType;
import com.bytefacets.spinel.grpc.proto.SchemaUpdate;
import com.bytefacets.spinel.grpc.proto.StringData;
import com.bytefacets.spinel.grpc.proto.SubscriptionResponse;
import com.bytefacets.spinel.schema.BoolField;
import com.bytefacets.spinel.schema.ByteField;
import com.bytefacets.spinel.schema.ChangedFieldSet;
import com.bytefacets.spinel.schema.CharField;
import com.bytefacets.spinel.schema.DoubleField;
import com.bytefacets.spinel.schema.FieldBitSet;
import com.bytefacets.spinel.schema.FloatField;
import com.bytefacets.spinel.schema.GenericField;
import com.bytefacets.spinel.schema.IntField;
import com.bytefacets.spinel.schema.LongField;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.SchemaField;
import com.bytefacets.spinel.schema.ShortField;
import com.bytefacets.spinel.schema.StringField;
import com.bytefacets.spinel.schema.TypeId;
import com.google.protobuf.ByteString;
import java.util.BitSet;

public final class GrpcEncoder implements ChangeEncoder<SubscriptionResponse> {
    private final ObjectEncoderImpl objectEncoder = ObjectEncoderImpl.encoder();
    private final BitSet allFields = new BitSet();
    private final FieldBitSet allFieldsChanges = FieldBitSet.fieldBitSet(allFields);
    private final IntVector rowIdBuffer = new IntVector(64);
    private final int subscriptionId;
    private byte[] byteArray = new byte[64];
    private Schema schema;

    private GrpcEncoder(final int subscriptionId) {
        this.subscriptionId = subscriptionId;
    }

    public static GrpcEncoder grpcEncoder(final int subscriptionId) {
        return new GrpcEncoder(subscriptionId);
    }

    @Override
    public SubscriptionResponse encodeSchema(final Schema schema) {
        this.schema = schema;
        allFields.clear();
        final var update =
                SubscriptionResponse.newBuilder()
                        .setResponseType(ResponseType.RESPONSE_TYPE_SCHEMA);
        final var builder = SchemaUpdate.newBuilder();
        if (schema != null) {
            allFields.set(0, schema.size());
            builder.setName(schema.name());
            schema.forEachField(f -> builder.addFields(fieldDefinition(f)));
        }
        return update.setSubscriptionId(subscriptionId)
                .setRefMsgToken(-1)
                .setSchema(builder.build())
                .build();
    }

    @Override
    public SubscriptionResponse encodeAdd(final IntIterable rows) {
        final var update =
                SubscriptionResponse.newBuilder().setResponseType(ResponseType.RESPONSE_TYPE_ADD);
        final var builder = DataUpdate.newBuilder();
        captureRows(rows, builder);
        captureData(builder, rowIdBuffer, allFieldsChanges);
        return update.setSubscriptionId(subscriptionId)
                .setRefMsgToken(-1)
                .setData(builder.build())
                .build();
    }

    @Override
    public SubscriptionResponse encodeChange(
            final IntIterable rows, final ChangedFieldSet fieldSet) {
        final var update =
                SubscriptionResponse.newBuilder().setResponseType(ResponseType.RESPONSE_TYPE_CHG);
        final var builder = DataUpdate.newBuilder();
        captureRows(rows, builder);
        captureData(builder, rowIdBuffer, fieldSet);
        return update.setSubscriptionId(subscriptionId)
                .setRefMsgToken(-1)
                .setData(builder.build())
                .build();
    }

    @Override
    public SubscriptionResponse encodeRemove(final IntIterable rows) {
        final var update =
                SubscriptionResponse.newBuilder().setResponseType(ResponseType.RESPONSE_TYPE_REM);
        final var builder = DataUpdate.newBuilder();
        captureRows(rows, builder);
        return update.setSubscriptionId(subscriptionId)
                .setRefMsgToken(-1)
                .setData(builder.build())
                .build();
    }

    private void captureRows(final IntIterable rows, final DataUpdate.Builder builder) {
        rowIdBuffer.clear();
        rows.forEach(
                row -> {
                    rowIdBuffer.append(row);
                    builder.addRows(row);
                });
    }

    private FieldDefinition fieldDefinition(final SchemaField f) {
        final var fb = FieldDefinition.newBuilder().setName(f.name()).setTypeId(f.typeId());
        if (f.metadata() != null) {
            fb.setMetadata(meta(f.metadata()));
        }
        return fb.build();
    }

    private Metadata meta(final com.bytefacets.spinel.schema.Metadata metadata) {
        final var mb = Metadata.newBuilder();
        if (metadata.tags() != null && !metadata.tags().isEmpty()) {
            mb.addAllTags(metadata.tags());
        }
        if (metadata.attributes() != null && !metadata.attributes().isEmpty()) {
            metadata.attributes()
                    .forEach(
                            (key, attr) -> {
                                final var result = objectEncoder.encode(attr);
                                if (!result.isEmpty()) {
                                    mb.putAttributes(key, result);
                                }
                            });
        }
        return mb.build();
    }

    @SuppressWarnings("CyclomaticComplexity")
    private void captureData(
            final DataUpdate.Builder builder,
            final IntVector rowIds,
            final ChangedFieldSet fieldSet) {
        fieldSet.forEach(
                fieldId -> {
                    final SchemaField field = schema.fieldAt(fieldId);
                    switch (field.typeId()) {
                        case TypeId.Bool -> addBoolField(builder, rowIds, field);
                        case TypeId.Byte -> addByteField(builder, rowIds, field);
                        case TypeId.Short -> addShortField(builder, rowIds, field);
                        case TypeId.Char -> addCharField(builder, rowIds, field);
                        case TypeId.Int -> addIntField(builder, rowIds, field);
                        case TypeId.Long -> addLongField(builder, rowIds, field);
                        case TypeId.Float -> addFloatField(builder, rowIds, field);
                        case TypeId.Double -> addDoubleField(builder, rowIds, field);
                        case TypeId.String -> addStringField(builder, rowIds, field);
                        case TypeId.Generic -> addGenericField(builder, rowIds, field);
                        default -> logUnknownFieldTypeId(field);
                    }
                });
    }

    private void logUnknownFieldTypeId(final SchemaField field) {
        // UPCOMING logUnknownFieldTypeId
    }

    private void addBoolField(
            final DataUpdate.Builder builder, final IntVector rowIds, final SchemaField field) {
        final BoolField f = (BoolField) field.field();
        final BoolData.Builder data = BoolData.newBuilder().setFieldId(field.fieldId());
        rowIds.forEach(row -> data.addValues(f.valueAt(row)));
        builder.addBoolData(data.build());
    }

    private void addShortField(
            final DataUpdate.Builder builder, final IntVector rowIds, final SchemaField field) {
        final ShortField f = (ShortField) field.field();
        final Int32Data.Builder data = Int32Data.newBuilder().setFieldId(field.fieldId());
        rowIds.forEach(row -> data.addValues(f.valueAt(row)));
        builder.addInt32Data(data.build());
    }

    private void addCharField(
            final DataUpdate.Builder builder, final IntVector rowIds, final SchemaField field) {
        final CharField f = (CharField) field.field();
        final Int32Data.Builder data = Int32Data.newBuilder().setFieldId(field.fieldId());
        rowIds.forEach(row -> data.addValues(f.valueAt(row)));
        builder.addInt32Data(data.build());
    }

    private void addIntField(
            final DataUpdate.Builder builder, final IntVector rowIds, final SchemaField field) {
        final IntField f = (IntField) field.field();
        final Int32Data.Builder data = Int32Data.newBuilder().setFieldId(field.fieldId());
        rowIds.forEach(row -> data.addValues(f.valueAt(row)));
        builder.addInt32Data(data.build());
    }

    private void addLongField(
            final DataUpdate.Builder builder, final IntVector rowIds, final SchemaField field) {
        final LongField f = (LongField) field.field();
        final Int64Data.Builder data = Int64Data.newBuilder().setFieldId(field.fieldId());
        rowIds.forEach(row -> data.addValues(f.valueAt(row)));
        builder.addInt64Data(data.build());
    }

    private void addFloatField(
            final DataUpdate.Builder builder, final IntVector rowIds, final SchemaField field) {
        final FloatField f = (FloatField) field.field();
        final FloatData.Builder data = FloatData.newBuilder().setFieldId(field.fieldId());
        rowIds.forEach(row -> data.addValues(f.valueAt(row)));
        builder.addFloatData(data.build());
    }

    private void addDoubleField(
            final DataUpdate.Builder builder, final IntVector rowIds, final SchemaField field) {
        final DoubleField f = (DoubleField) field.field();
        final DoubleData.Builder data = DoubleData.newBuilder().setFieldId(field.fieldId());
        rowIds.forEach(row -> data.addValues(f.valueAt(row)));
        builder.addDoubleData(data.build());
    }

    private void addStringField(
            final DataUpdate.Builder builder, final IntVector rowIds, final SchemaField field) {
        final StringField f = (StringField) field.field();
        final StringData.Builder data = StringData.newBuilder().setFieldId(field.fieldId());
        rowIds.forEach(row -> data.addValues(f.valueAt(row)));
        builder.addStringData(data.build());
    }

    private void addGenericField(
            final DataUpdate.Builder builder, final IntVector rowIds, final SchemaField field) {
        final GenericField f = (GenericField) field.field();
        final GenericData.Builder data = GenericData.newBuilder().setFieldId(field.fieldId());
        rowIds.forEach(row -> data.addValues(objectEncoder.encode(f.valueAt(row))));
        builder.addGenericData(data.build());
    }

    private void addByteField(
            final DataUpdate.Builder builder, final IntVector rowIds, final SchemaField field) {
        final ByteField f = (ByteField) field.field();
        final ByteData.Builder data = ByteData.newBuilder().setFieldId(field.fieldId());
        byteArray = ByteArray.ensureSize(byteArray, rowIds.size());
        for (int i = 0, len = byteArray.length; i < len; i++) {
            byteArray[i] = f.valueAt(rowIds.valueAt(i));
        }
        data.setValues(ByteString.copyFrom(byteArray, 0, rowIds.size()));
        builder.addByteData(data.build());
    }
}
