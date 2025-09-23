// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.schema;

import java.util.HashMap;
import java.util.Map;

public final class MetadataBuilder {
    private HashMap<String, Object> attrs;

    private MetadataBuilder() {}

    public static MetadataBuilder metadataBuilder() {
        return new MetadataBuilder();
    }

    public MetadataBuilder displayPrecision(final int value) {
        AttributeConstants.setDisplayPrecision(getOrCreateAttrMap(), (byte) value);
        return this;
    }

    public MetadataBuilder displayFormat(final String value) {
        AttributeConstants.setDisplayFormat(getOrCreateAttrMap(), value);
        return this;
    }

    public MetadataBuilder contentType(final byte value) {
        AttributeConstants.setContentType(getOrCreateAttrMap(), value);
        return this;
    }

    public MetadataBuilder timeZone(final String value) {
        AttributeConstants.setTimeZone(getOrCreateAttrMap(), value);
        return this;
    }

    public MetadataBuilder valuePrecision(final int value) {
        AttributeConstants.setValuePrecision(getOrCreateAttrMap(), (byte) value);
        return this;
    }

    private Map<String, Object> getOrCreateAttrMap() {
        if (attrs == null) {
            attrs = new HashMap<>(4);
        }
        return attrs;
    }

    public Metadata build() {
        return Metadata.metadata(attrs);
    }
}
