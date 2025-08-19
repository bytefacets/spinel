// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.grpc.receive;

import com.bytefacets.diaspore.grpc.proto.DataUpdate;
import com.bytefacets.diaspore.grpc.proto.ResponseType;
import com.bytefacets.diaspore.schema.FieldList;
import java.util.BitSet;

interface TypeReader {
    /**
     * @param fields the fields into which data will be written
     * @param changedFieldIds the field ids object to collect changed fields referenced by the
     *     message
     */
    void setContext(FieldList fields, BitSet changedFieldIds);

    /**
     * Reads a DataUpdate by marks the changedFields in the case of row updates, and writes to the
     * fields available in the context's fieldList.
     *
     * @param msg the inbound message referencing rows and data
     * @param op the type of message (Add, Change, Remove)
     */
    void read(DataUpdate msg, ResponseType op);
}
