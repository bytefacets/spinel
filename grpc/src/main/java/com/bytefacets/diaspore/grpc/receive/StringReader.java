// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.grpc.receive;

import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.grpc.proto.DataUpdate;
import com.bytefacets.diaspore.grpc.proto.ResponseType;
import com.bytefacets.diaspore.grpc.proto.StringData;
import com.bytefacets.diaspore.schema.FieldList;
import com.bytefacets.diaspore.schema.SchemaField;
import com.bytefacets.diaspore.schema.StringWritableField;
import java.util.BitSet;

final class StringReader implements TypeReader {
    private FieldList fields;
    private BitSet changedFieldIds;

    @Override
    public void setContext(final FieldList fields, final BitSet changedFieldIds) {
        this.fields = requireNonNull(fields, "fields");
        this.changedFieldIds = requireNonNull(changedFieldIds, "changedFieldIds");
    }

    @Override
    public void read(final DataUpdate msg, final ResponseType op) {
        if (msg.getStringDataCount() != 0) {
            for (int d = 0, len = msg.getStringDataCount(); d < len; d++) {
                final var data = msg.getStringData(d);
                if (op.equals(ResponseType.RESPONSE_TYPE_CHG)) {
                    changedFieldIds.set(data.getFieldId());
                }
                readField(msg, data);
            }
        }
    }

    private void readField(final DataUpdate msg, final StringData data) {
        final int fieldId = data.getFieldId();
        final SchemaField schemaField = fields.fieldAt(fieldId);
        final var field = (StringWritableField) schemaField.field();
        final int rowCt = msg.getRowsCount();
        for (int i = 0; i < rowCt; i++) {
            field.setValueAt(msg.getRows(i), data.getValues(i));
        }
    }
}
