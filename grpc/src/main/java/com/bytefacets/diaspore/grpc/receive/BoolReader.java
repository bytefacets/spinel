// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.grpc.receive;

import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.grpc.proto.BoolData;
import com.bytefacets.diaspore.grpc.proto.DataUpdate;
import com.bytefacets.diaspore.grpc.proto.ResponseType;
import com.bytefacets.diaspore.schema.BoolWritableField;
import com.bytefacets.diaspore.schema.FieldList;
import java.util.BitSet;

final class BoolReader implements TypeReader {
    private FieldList fields;
    private BitSet changedFieldIds;

    @Override
    public void setContext(final FieldList fields, final BitSet changedFieldIds) {
        this.fields = requireNonNull(fields, "fields");
        this.changedFieldIds = requireNonNull(changedFieldIds, "changedFieldIds");
    }

    @Override
    public void read(final DataUpdate msg, final ResponseType op) {
        if (msg.getBoolDataCount() != 0) {
            for (int d = 0, len = msg.getBoolDataCount(); d < len; d++) {
                final BoolData data = msg.getBoolData(d);
                if (op.equals(ResponseType.RESPONSE_TYPE_CHG)) {
                    changedFieldIds.set(data.getFieldId());
                }
                readField(msg, data);
            }
        }
    }

    private void readField(final DataUpdate msg, final BoolData data) {
        final int fieldId = data.getFieldId();
        final var field = (BoolWritableField) fields.fieldAt(fieldId).field();
        final int rowCt = msg.getRowsCount();
        for (int i = 0; i < rowCt; i++) {
            field.setValueAt(msg.getRows(i), data.getValues(i));
        }
    }
}
