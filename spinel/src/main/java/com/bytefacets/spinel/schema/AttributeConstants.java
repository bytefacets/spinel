// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.schema;

import com.bytefacets.collections.types.ByteType;
import java.time.ZoneId;
import java.util.Map;

public final class AttributeConstants {
    private AttributeConstants() {}

    // formatting:off
    public static final String ContentType      = "contyp";
    public static final String ValuePrecision   = "valprc";
    public static final String DisplayPrecision = "disprc";
    public static final String DisplayFormat    = "disfmt";
    public static final String TimeZone         = "tz";
    // formatting:on

    public static final class DisplayFormats {
        private DisplayFormats() {}

        /** Thousands (k), Millions, Billions, Trillions */
        public static final String kMBT = "kMBT";

        /** Little Endian, for numeric types when represented as text. Value is "LE" */
        public static final String LittleEndian = "LE";

        /** Big Endian, for numeric types when represented as text. Value is "BE" */
        public static final String BigEndian = "BE";
    }

    public static final class ContentTypes {
        private ContentTypes() {}

        // formatting:off
        public static final byte Natural   =  0;
        public static final byte Text      =  1;
        public static final byte Date      =  2;
        public static final byte Time      =  3;
        public static final byte Timestamp =  4;
        public static final byte Quantity  =  5;
        public static final byte Duration  =  6;
        public static final byte Percent   =  7;
        public static final byte Id        =  8;
        public static final byte Packed2   =  9;
        public static final byte Packed4   = 10;
        public static final byte Flag      = 11;
        // formatting:on
    }

    public static final class Precisions {
        private Precisions() {}

        public static final class Timestamp {
            private Timestamp() {}

            // formatting:off
            public static final byte Second =  1;
            public static final byte Milli  = -3;
            public static final byte Micro  = -6;
            public static final byte Nano   = -9;
            // formatting:on
        }
    }

    public static void setContentType(final Map<String, Object> attributes, final byte type) {
        attributes.put(ContentType, type);
    }

    public static void setValuePrecision(
            final Map<String, Object> attributes, final byte precision) {
        attributes.put(ValuePrecision, precision);
    }

    public static void setDisplayPrecision(
            final Map<String, Object> attributes, final byte precision) {
        attributes.put(DisplayPrecision, precision);
    }

    public static void setDisplayFormat(final Map<String, Object> attributes, final String format) {
        attributes.put(DisplayFormat, format);
    }

    public static void setTimeZone(final Map<String, Object> attributes, final ZoneId zone) {
        attributes.put(TimeZone, zone.getId());
    }

    public static void setTimeZone(final Map<String, Object> attributes, final String zone) {
        attributes.put(TimeZone, zone);
    }

    public static byte contentType(final Metadata metadata) {
        return ByteType.convert(
                metadata.attributes().getOrDefault(ContentType, ContentTypes.Natural));
    }

    public static byte valuePrecision(final Metadata metadata, final byte defaultValue) {
        return ByteType.convert(metadata.attributes().getOrDefault(ValuePrecision, defaultValue));
    }

    public static byte displayPrecision(final Metadata metadata, final byte defaultValue) {
        return ByteType.convert(metadata.attributes().getOrDefault(DisplayPrecision, defaultValue));
    }

    public static String displayFormat(final Metadata metadata) {
        final Object value = metadata.attributes().get(DisplayFormat);
        return value != null ? value.toString() : null;
    }

    public static String timeZone(final Metadata metadata) {
        final Object value = metadata.attributes().get(TimeZone);
        return value != null ? value.toString() : null;
    }
}
