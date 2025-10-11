// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.schema;

import static com.bytefacets.spinel.schema.Metadata.metadata;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Map;
import org.junit.jupiter.api.Test;

class MetadataTest {
    @Test
    void shouldUpdateAttributes() {
        final var updated =
                metadata(Map.of("x", 1, "y", 2)).update(metadata(Map.of("x", 10, "z", 30)));
        assertThat(updated, equalTo(metadata(Map.of("x", 10, "y", 2, "z", 30))));
    }
}
