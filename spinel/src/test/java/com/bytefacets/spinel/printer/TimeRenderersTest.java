// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.printer;

import static com.bytefacets.spinel.printer.TimeRenderers.durationRenderer;
import static com.bytefacets.spinel.printer.TimeRenderers.timeRenderer;
import static com.bytefacets.spinel.printer.TimeRenderers.timestampRenderer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.bytefacets.spinel.schema.AttributeConstants;
import com.bytefacets.spinel.schema.Metadata;
import java.time.Duration;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class TimeRenderersTest {
    private final Map<String, Object> attrs = new HashMap<>();
    private final StringBuilder sb = new StringBuilder();

    @Nested
    class TimestampTests {
        @BeforeEach
        void setUp() {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Timestamp);
        }

        @ParameterizedTest
        @CsvSource({
            "-3,2025-09-15 21:46:37.123",
            "-6,2025-09-15 21:46:37.123456",
            "-9,2025-09-15 21:46:37.123456789",
        })
        void shouldRenderNanoTimestamps(final byte displayPrecision, final String expected) {
            AttributeConstants.setDisplayPrecision(attrs, displayPrecision);
            assertThat(render(1757972797123456789L), equalTo(expected));
        }

        @ParameterizedTest
        @CsvSource({
            "-3,2025-09-15 21:46:37.123",
            "-6,2025-09-15 21:46:37.123456",
            "-9,2025-09-15 21:46:37.123456000",
        })
        void shouldRenderMicroTimestamps(final byte displayPrecision, final String expected) {
            AttributeConstants.setDisplayPrecision(attrs, displayPrecision);
            AttributeConstants.setValuePrecision(
                    attrs, AttributeConstants.Precisions.Timestamp.Micro);
            assertThat(render(1757972797123456L), equalTo(expected));
        }

        @ParameterizedTest
        @CsvSource({
            "-3,2025-09-15 21:46:37.123",
            "-6,2025-09-15 21:46:37.123000",
            "-9,2025-09-15 21:46:37.123000000",
        })
        void shouldRenderMilliTimestamps(final byte displayPrecision, final String expected) {
            AttributeConstants.setDisplayPrecision(attrs, displayPrecision);
            AttributeConstants.setValuePrecision(
                    attrs, AttributeConstants.Precisions.Timestamp.Milli);
            assertThat(render(1757972797123L), equalTo(expected));
        }

        @ParameterizedTest
        @CsvSource({
            "-3,2025-09-15 21:46:37.000",
            "-6,2025-09-15 21:46:37.000000",
            "-9,2025-09-15 21:46:37.000000000",
        })
        void shouldRenderSecondTimestamps(final byte displayPrecision, final String expected) {
            AttributeConstants.setDisplayPrecision(attrs, displayPrecision);
            AttributeConstants.setValuePrecision(
                    attrs, AttributeConstants.Precisions.Timestamp.Second);
            assertThat(render(1757972797L), equalTo(expected));
        }

        @Test
        void shouldUseZones() {
            AttributeConstants.setTimeZone(attrs, ZoneId.of("America/New_York"));
            assertThat(render(1757972797123456789L), equalTo("2025-09-15 17:46:37.123456789"));
        }

        private String render(final long value) {
            sb.setLength(0);
            timestampRenderer(metadata()).renderValue(sb, value);
            return sb.toString();
        }
    }

    @Nested
    class TimeTests {
        @BeforeEach
        void setUp() {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Time);
        }

        @ParameterizedTest
        @CsvSource({
            "-3,21:46:37.123",
            "-6,21:46:37.123456",
            "-9,21:46:37.123456789",
        })
        void shouldRenderNanoTimestamps(final byte displayPrecision, final String expected) {
            AttributeConstants.setDisplayPrecision(attrs, displayPrecision);
            assertThat(render(1757972797123456789L), equalTo(expected));
        }

        @ParameterizedTest
        @CsvSource({
            "-3,21:46:37.123",
            "-6,21:46:37.123456",
            "-9,21:46:37.123456000",
        })
        void shouldRenderMicroTimestamps(final byte displayPrecision, final String expected) {
            AttributeConstants.setDisplayPrecision(attrs, displayPrecision);
            AttributeConstants.setValuePrecision(
                    attrs, AttributeConstants.Precisions.Timestamp.Micro);
            assertThat(render(1757972797123456L), equalTo(expected));
        }

        @ParameterizedTest
        @CsvSource({
            "-3,21:46:37.123",
            "-6,21:46:37.123000",
            "-9,21:46:37.123000000",
        })
        void shouldRenderMilliTimestamps(final byte displayPrecision, final String expected) {
            AttributeConstants.setDisplayPrecision(attrs, displayPrecision);
            AttributeConstants.setValuePrecision(
                    attrs, AttributeConstants.Precisions.Timestamp.Milli);
            assertThat(render(1757972797123L), equalTo(expected));
        }

        @ParameterizedTest
        @CsvSource({
            "-3,21:46:37.000",
            "-6,21:46:37.000000",
            "-9,21:46:37.000000000",
        })
        void shouldRenderSecondTimestamps(final byte displayPrecision, final String expected) {
            AttributeConstants.setDisplayPrecision(attrs, displayPrecision);
            AttributeConstants.setValuePrecision(
                    attrs, AttributeConstants.Precisions.Timestamp.Second);
            assertThat(render(1757972797L), equalTo(expected));
        }

        @Test
        void shouldUseZones() {
            AttributeConstants.setTimeZone(attrs, ZoneId.of("America/New_York"));
            assertThat(render(1757972797123456789L), equalTo("17:46:37.123456789"));
        }

        private String render(final long value) {
            sb.setLength(0);
            timeRenderer(metadata()).renderValue(sb, value);
            return sb.toString();
        }
    }

    @Nested
    class DurationTests {
        @BeforeEach
        void setUp() {
            AttributeConstants.setContentType(attrs, AttributeConstants.ContentTypes.Duration);
        }

        @ParameterizedTest
        @CsvSource({
            "1,12:34:56",
            "-3,12:34:56.123",
            "-6,12:34:56.123456",
            "-9,12:34:56.123456789",
        })
        void shouldRenderNanoTimestamps(final byte displayPrecision, final String expected) {
            AttributeConstants.setDisplayPrecision(attrs, displayPrecision);
            assertThat(render(toNanos(12, 34, 56, 123456789)), equalTo(expected));
        }

        @ParameterizedTest
        @CsvSource({
            "1,12:34:56",
            "-3,12:34:56.123",
            "-6,12:34:56.123456",
            "-9,12:34:56.123456000",
        })
        void shouldRenderMicroTimestamps(final byte displayPrecision, final String expected) {
            AttributeConstants.setDisplayPrecision(attrs, displayPrecision);
            AttributeConstants.setValuePrecision(
                    attrs, AttributeConstants.Precisions.Timestamp.Micro);
            assertThat(render(toNanos(12, 34, 56, 123456789) / 1000L), equalTo(expected));
        }

        @ParameterizedTest
        @CsvSource({
            "1,12:34:56",
            "-3,12:34:56.123",
            "-6,12:34:56.123000",
            "-9,12:34:56.123000000",
        })
        void shouldRenderMilliTimestamps(final byte displayPrecision, final String expected) {
            AttributeConstants.setDisplayPrecision(attrs, displayPrecision);
            AttributeConstants.setValuePrecision(
                    attrs, AttributeConstants.Precisions.Timestamp.Milli);
            assertThat(render(toNanos(12, 34, 56, 123456789) / 1_000_000L), equalTo(expected));
        }

        @ParameterizedTest
        @CsvSource({
            "1,12:34:56",
            "-3,12:34:56.000",
            "-6,12:34:56.000000",
            "-9,12:34:56.000000000",
        })
        void shouldRenderSecondTimestamps(final byte displayPrecision, final String expected) {
            AttributeConstants.setDisplayPrecision(attrs, displayPrecision);
            AttributeConstants.setValuePrecision(
                    attrs, AttributeConstants.Precisions.Timestamp.Second);
            assertThat(render(toNanos(12, 34, 56, 123456789) / 1_000_000_000L), equalTo(expected));
        }

        @Test
        void shouldZeroPad() {
            assertThat(render(toNanos(2, 34, 56, 123456789)), equalTo("02:34:56.123456789"));
            assertThat(render(toNanos(2, 34, 56, 9)), equalTo("02:34:56.000000009"));
            assertThat(render(toNanos(2, 34, 56, 1234567)), equalTo("02:34:56.001234567"));
            assertThat(render(toNanos(2, 34, 6, 123456789)), equalTo("02:34:06.123456789"));
            assertThat(render(toNanos(2, 4, 56, 123456789)), equalTo("02:04:56.123456789"));
            assertThat(render(toNanos(0, 34, 56, 123456789)), equalTo("00:34:56.123456789"));
            assertThat(render(toNanos(0, 0, 56, 123456789)), equalTo("00:00:56.123456789"));
            assertThat(render(toNanos(0, 0, 0, 123456789)), equalTo("00:00:00.123456789"));
        }

        @Test
        void shouldNotAppendAnythingWhenAtMinOrMax() {
            assertThat(render(Long.MAX_VALUE), equalTo(""));
            assertThat(render(Long.MIN_VALUE), equalTo(""));
        }

        private String render(final long value) {
            sb.setLength(0);
            durationRenderer(metadata()).renderValue(sb, value);
            return sb.toString();
        }

        private long toNanos(
                final int hours, final int minutes, final int seconds, final int nanos) {
            return Duration.ofHours(hours).toNanos()
                    + Duration.ofMinutes(minutes).toNanos()
                    + Duration.ofSeconds(seconds).toNanos()
                    + nanos;
        }
    }

    private Metadata metadata() {
        return Metadata.metadata(attrs);
    }
}
