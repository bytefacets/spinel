// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.grpc.receive;

import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.grpc.proto.DataUpdate;
import com.bytefacets.diaspore.grpc.proto.FloatData;
import com.bytefacets.diaspore.grpc.proto.ResponseType;
import com.bytefacets.diaspore.schema.FieldList;
import com.bytefacets.diaspore.schema.FloatWritableField;
import com.bytefacets.diaspore.schema.SchemaField;
import java.util.BitSet;

final class FloatReader implements TypeReader {
    private FieldList fields;
    private BitSet changedFieldIds;

    @Override
    public void setContext(final FieldList fields, final BitSet changedFieldIds) {
        this.fields = requireNonNull(fields, "fields");
        this.changedFieldIds = requireNonNull(changedFieldIds, "changedFieldIds");
    }

    @Override
    public void read(final DataUpdate msg, final ResponseType op) {
        if (msg.getFloatDataCount() != 0) {
            for (int d = 0, len = msg.getFloatDataCount(); d < len; d++) {
                final var data = msg.getFloatData(d);
                if (op.equals(ResponseType.RESPONSE_TYPE_CHG)) {
                    changedFieldIds.set(data.getFieldId());
                }
                readField(msg, data);
            }
        }
    }

    private void readField(final DataUpdate msg, final FloatData data) {
        final int fieldId = data.getFieldId();
        final SchemaField schemaField = fields.fieldAt(fieldId);
        final var field = (FloatWritableField) schemaField.field();
        final int rowCt = msg.getRowsCount();
        for (int i = 0; i < rowCt; i++) {
            field.setValueAt(msg.getRows(i), data.getValues(i));
        }
    }
}
