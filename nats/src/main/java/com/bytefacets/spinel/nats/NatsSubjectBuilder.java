// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.nats;

import com.bytefacets.spinel.schema.SchemaBindable;

public interface NatsSubjectBuilder extends SchemaBindable {
    String buildSubject(int row);
}
