package com.bytefacets.diaspore.grpc.receive;

import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.grpc.ByteData;
import com.bytefacets.diaspore.grpc.DataUpdate;
import com.bytefacets.diaspore.grpc.ResponseType;
import com.bytefacets.diaspore.schema.ByteWritableField;
import com.bytefacets.diaspore.schema.FieldList;
import com.google.protobuf.ByteString;
import java.util.BitSet;

final class ByteReader implements TypeReader {
    private FieldList fields;
    private BitSet changedFieldIds;

    @Override
    public void setContext(final FieldList fields, final BitSet changedFieldIds) {
        this.fields = requireNonNull(fields, "fields");
        this.changedFieldIds = requireNonNull(changedFieldIds, "changedFieldIds");
    }

    @Override
    public void read(final DataUpdate msg, final ResponseType op) {
        if (msg.getByteDataCount() != 0) {
            for (int d = 0, len = msg.getByteDataCount(); d < len; d++) {
                final var data = msg.getByteData(d);
                if (op.equals(ResponseType.RESPONSE_TYPE_CHG)) {
                    changedFieldIds.set(data.getFieldId());
                }
                readField(msg, data);
            }
        }
    }

    private void readField(final DataUpdate msg, final ByteData data) {
        final int fieldId = data.getFieldId();
        final ByteString byteValues = data.getValues();
        final var field = (ByteWritableField) fields.fieldAt(fieldId).field();
        final int rowCt = msg.getRowsCount();
        for (int i = 0; i < rowCt; i++) {
            field.setValueAt(msg.getRows(i), byteValues.byteAt(i));
        }
    }
}
