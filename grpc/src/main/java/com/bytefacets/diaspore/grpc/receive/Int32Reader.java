package com.bytefacets.diaspore.grpc.receive;

import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.grpc.DataUpdate;
import com.bytefacets.diaspore.grpc.Int32Data;
import com.bytefacets.diaspore.grpc.ResponseType;
import com.bytefacets.diaspore.schema.CharWritableField;
import com.bytefacets.diaspore.schema.FieldList;
import com.bytefacets.diaspore.schema.IntWritableField;
import com.bytefacets.diaspore.schema.SchemaField;
import com.bytefacets.diaspore.schema.ShortWritableField;
import com.bytefacets.diaspore.schema.TypeId;
import java.util.BitSet;

final class Int32Reader implements TypeReader {
    private FieldList fields;
    private BitSet changedFieldIds;

    @Override
    public void setContext(final FieldList fields, final BitSet changedFieldIds) {
        this.fields = requireNonNull(fields, "fields");
        this.changedFieldIds = requireNonNull(changedFieldIds, "changedFieldIds");
    }

    @Override
    public void read(final DataUpdate msg, final ResponseType op) {
        if (msg.getInt32DataCount() != 0) {
            for (int d = 0, len = msg.getInt32DataCount(); d < len; d++) {
                final var data = msg.getInt32Data(d);
                if (op.equals(ResponseType.RESPONSE_TYPE_CHG)) {
                    changedFieldIds.set(data.getFieldId());
                }
                readField(msg, data);
            }
        }
    }

    private void readField(final DataUpdate msg, final Int32Data data) {
        final int fieldId = data.getFieldId();
        final SchemaField schemaField = fields.fieldAt(fieldId);
        switch (schemaField.typeId()) {
            case TypeId.Char:
                {
                    applyChar(msg, data, (CharWritableField) schemaField.field());
                    break;
                }
            case TypeId.Short:
                {
                    applyShort(msg, data, (ShortWritableField) schemaField.field());
                    break;
                }
            case TypeId.Int:
                {
                    applyInt(msg, data, (IntWritableField) schemaField.field());
                    break;
                }
        }
    }

    private void applyChar(
            final DataUpdate msg, final Int32Data data, final CharWritableField field) {
        final int rowCt = msg.getRowsCount();
        for (int i = 0; i < rowCt; i++) {
            field.setValueAt(msg.getRows(i), (char) data.getValues(i));
        }
    }

    private void applyShort(
            final DataUpdate msg, final Int32Data data, final ShortWritableField field) {
        final int rowCt = msg.getRowsCount();
        for (int i = 0; i < rowCt; i++) {
            field.setValueAt(msg.getRows(i), (short) data.getValues(i));
        }
    }

    private void applyInt(
            final DataUpdate msg, final Int32Data data, final IntWritableField field) {
        final int rowCt = msg.getRowsCount();
        for (int i = 0; i < rowCt; i++) {
            field.setValueAt(msg.getRows(i), data.getValues(i));
        }
    }
}
