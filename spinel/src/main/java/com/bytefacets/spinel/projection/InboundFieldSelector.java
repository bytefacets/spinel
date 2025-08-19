// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.projection;

import com.bytefacets.spinel.schema.FieldList;
import com.bytefacets.spinel.schema.SchemaField;
import java.util.Map;

interface InboundFieldSelector {
    Map<String, SchemaField> selectFields(String name, FieldList inboundFields);
}
