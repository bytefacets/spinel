// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.receive;

import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.grpc.proto.DataUpdate;
import com.bytefacets.spinel.grpc.proto.DoubleData;
import com.bytefacets.spinel.grpc.proto.ResponseType;
import com.bytefacets.spinel.schema.DoubleWritableField;
import com.bytefacets.spinel.schema.FieldList;
import java.util.BitSet;

final class DoubleReader implements TypeReader {
    private FieldList fields;
    private BitSet changedFieldIds;

    @Override
    public void setContext(final FieldList fields, final BitSet changedFieldIds) {
        this.fields = requireNonNull(fields, "fields");
        this.changedFieldIds = requireNonNull(changedFieldIds, "changedFieldIds");
    }

    @Override
    public void read(final DataUpdate msg, final ResponseType op) {
        if (msg.getDoubleDataCount() != 0) {
            for (int d = 0, len = msg.getDoubleDataCount(); d < len; d++) {
                final var data = msg.getDoubleData(d);
                if (op.equals(ResponseType.RESPONSE_TYPE_CHG)) {
                    changedFieldIds.set(data.getFieldId());
                }
                readField(msg, data);
            }
        }
    }

    private void readField(final DataUpdate msg, final DoubleData data) {
        final int fieldId = data.getFieldId();
        final var schemaField = fields.fieldAt(fieldId);
        final var field = (DoubleWritableField) schemaField.field();
        final int rowCt = msg.getRowsCount();
        for (int i = 0; i < rowCt; i++) {
            field.setValueAt(msg.getRows(i), data.getValues(i));
        }
    }
}
