// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.projection;

import com.bytefacets.diaspore.schema.FieldList;
import com.bytefacets.diaspore.schema.SchemaField;
import java.util.Map;

interface InboundFieldSelector {
    Map<String, SchemaField> selectFields(String name, FieldList inboundFields);
}
