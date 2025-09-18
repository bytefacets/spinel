// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.printer;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

import com.bytefacets.spinel.schema.AttributeConstants;
import com.bytefacets.spinel.schema.Metadata;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@SuppressWarnings({"CyclomaticComplexity", "NPathComplexity", "NeedBraces"})
final class TimeRenderers {
    private TimeRenderers() {}

    private static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
    private static final ZoneId UTC = ZoneId.of("UTC");
    private static final Map<ChronoUnit, DateTimeFormatter> DATE_TIME_FORMATTERS =
            Map.of(
                    ChronoUnit.NANOS, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
                    ChronoUnit.MICROS, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
                    ChronoUnit.MILLIS, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
                    ChronoUnit.SECONDS, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    private static final Map<ChronoUnit, DateTimeFormatter> TIME_FORMATTERS =
            Map.of(
                    ChronoUnit.NANOS, DateTimeFormatter.ofPattern("HH:mm:ss.SSS"),
                    ChronoUnit.MICROS, DateTimeFormatter.ofPattern("HH:mm:ss.SSS"),
                    ChronoUnit.MILLIS, DateTimeFormatter.ofPattern("HH:mm:ss.SSS"),
                    ChronoUnit.SECONDS, DateTimeFormatter.ofPattern("HH:mm:ss"),
                    ChronoUnit.MINUTES, DateTimeFormatter.ofPattern("HH:mm"),
                    ChronoUnit.HOURS, DateTimeFormatter.ofPattern("HH"));

    static long normalizeToNanos(final long value, final ChronoUnit valuePrecision) {
        return TimeUnit.of(requireNonNullElse(valuePrecision, ChronoUnit.NANOS)).toNanos(value);
    }

    static LongRenderers.RenderMethod timeRenderer(final Metadata metadata) {
        final ChronoUnit valuePrecision =
                toChronoUnit(
                        AttributeConstants.valuePrecision(
                                metadata, AttributeConstants.Precisions.Timestamp.Nano));
        final ChronoUnit displayPrecision =
                toChronoUnit(
                        AttributeConstants.displayPrecision(
                                metadata, AttributeConstants.Precisions.Timestamp.Nano));
        final ZoneId zone = toZoneId(AttributeConstants.timeZone(metadata));
        final DateTimeFormatter formatter = TIME_FORMATTERS.get(displayPrecision);
        return switch (displayPrecision) {
            case NANOS -> new NanoFormatter(valuePrecision, formatter, zone);
            case MICROS -> new MicroFormatter(valuePrecision, formatter, zone);
            default -> new TimeRelatedFormatterImpl(valuePrecision, formatter, zone);
        };
    }

    static LongRenderers.RenderMethod timestampRenderer(final Metadata metadata) {
        final ChronoUnit valuePrecision =
                toChronoUnit(
                        AttributeConstants.valuePrecision(
                                metadata, AttributeConstants.Precisions.Timestamp.Nano));
        final ChronoUnit displayPrecision =
                toChronoUnit(
                        AttributeConstants.displayPrecision(
                                metadata, AttributeConstants.Precisions.Timestamp.Nano));
        final ZoneId zone = toZoneId(AttributeConstants.timeZone(metadata));
        final DateTimeFormatter formatter = DATE_TIME_FORMATTERS.get(displayPrecision);
        return switch (displayPrecision) {
            case NANOS -> new NanoFormatter(valuePrecision, formatter, zone);
            case MICROS -> new MicroFormatter(valuePrecision, formatter, zone);
            default -> new TimeRelatedFormatterImpl(valuePrecision, formatter, zone);
        };
    }

    private abstract static class TimeRelatedFormatter {
        protected final ChronoUnit valuePrecision;
        protected final DateTimeFormatter formatter;
        protected final ZoneId zoneId;

        private TimeRelatedFormatter(
                final ChronoUnit valuePrecision,
                final DateTimeFormatter formatter,
                final ZoneId zoneId) {
            this.valuePrecision = requireNonNull(valuePrecision, "valuePrecision");
            this.formatter = requireNonNull(formatter, "formatter");
            this.zoneId = requireNonNull(zoneId, "zoneId");
        }
    }

    private static final class TimeRelatedFormatterImpl extends TimeRelatedFormatter
            implements LongRenderers.RenderMethod {
        private TimeRelatedFormatterImpl(
                final ChronoUnit valuePrecision,
                final DateTimeFormatter formatter,
                final ZoneId zoneId) {
            super(valuePrecision, formatter, zoneId);
        }

        @Override
        public void renderValue(final StringBuilder sb, final long value) {
            final long nanos = normalizeToNanos(value, valuePrecision);
            formatter.formatTo(toZonedDateTime(nanos, zoneId), sb);
        }
    }

    private static final class NanoFormatter extends TimeRelatedFormatter
            implements LongRenderers.RenderMethod {
        private NanoFormatter(
                final ChronoUnit valuePrecision,
                final DateTimeFormatter formatter,
                final ZoneId zoneId) {
            super(valuePrecision, formatter, zoneId);
        }

        @Override
        public void renderValue(final StringBuilder sb, final long value) {
            final long nanos = normalizeToNanos(value, valuePrecision);
            formatter.formatTo(toZonedDateTime(nanos, zoneId), sb);
            final long nanoValue = nanos % 1_000_000;
            sb.append(pads[6 - stringLength(nanoValue)]);
            sb.append(nanoValue);
        }
    }

    private static final class MicroFormatter extends TimeRelatedFormatter
            implements LongRenderers.RenderMethod {
        private MicroFormatter(
                final ChronoUnit valuePrecision,
                final DateTimeFormatter formatter,
                final ZoneId zoneId) {
            super(valuePrecision, formatter, zoneId);
        }

        @Override
        public void renderValue(final StringBuilder sb, final long value) {
            final long nanos = normalizeToNanos(value, valuePrecision);
            formatter.formatTo(toZonedDateTime(nanos, zoneId), sb);
            final long microValue = (nanos % 1_000_000) / 1_000;
            sb.append(pads[3 - stringLength(microValue)]);
            sb.append(microValue);
        }
    }

    static LongRenderers.RenderMethod durationRenderer(final Metadata metadata) {
        final ChronoUnit valuePrecision =
                toChronoUnit(
                        AttributeConstants.valuePrecision(
                                metadata, AttributeConstants.Precisions.Timestamp.Nano));
        final ChronoUnit displayPrecision =
                toChronoUnit(
                        AttributeConstants.displayPrecision(
                                metadata, AttributeConstants.Precisions.Timestamp.Nano));
        return (sb, value) -> {
            final long nanos = normalizeToNanos(value, valuePrecision);
            writeDuration(nanos, displayPrecision, sb);
        };
    }

    @SuppressWarnings({"NeedsBraces", "FinalParameters"})
    static void writeDuration(
            long nanos, final ChronoUnit displayPrecision, final StringBuilder sb) {
        if (nanos == Long.MIN_VALUE || nanos == Long.MAX_VALUE) {
            return; // don't output anything
        }

        final boolean showNanos = displayPrecision.equals(ChronoUnit.NANOS);
        final boolean showMicros = showNanos || displayPrecision.equals(ChronoUnit.MICROS);
        final boolean showMillis = showMicros || displayPrecision.equals(ChronoUnit.MILLIS);
        final boolean showSeconds = showMillis || displayPrecision.equals(ChronoUnit.SECONDS);

        if (nanos < 0) {
            sb.append('-');
            nanos = -nanos;
        }

        final long hours = TimeUnit.NANOSECONDS.toHours(nanos);
        nanos -= TimeUnit.HOURS.toNanos(hours);
        if (hours < 10) sb.append('0');
        sb.append(hours).append(':');

        final long minutes = TimeUnit.NANOSECONDS.toMinutes(nanos);
        nanos -= TimeUnit.MINUTES.toNanos(minutes);
        if (minutes < 10) sb.append('0');
        sb.append(minutes);

        if (showSeconds) {
            sb.append(':');
            final long seconds = TimeUnit.NANOSECONDS.toSeconds(nanos);
            nanos -= TimeUnit.SECONDS.toNanos(seconds);
            if (seconds < 10) sb.append('0');
            sb.append(seconds);
        }
        if (showMillis) {
            sb.append('.');
            final long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
            nanos -= TimeUnit.MILLISECONDS.toNanos(millis);
            if (millis < 100) sb.append('0');
            if (millis < 10) sb.append('0');
            sb.append(millis);
        }
        if (showMicros) {
            final long micros = TimeUnit.NANOSECONDS.toMicros(nanos);
            nanos -= TimeUnit.MICROSECONDS.toNanos(micros);
            if (micros < 100) sb.append('0');
            if (micros < 10) sb.append('0');
            sb.append(micros);
        }
        if (showNanos) {
            if (nanos < 100) sb.append('0');
            if (nanos < 10) sb.append('0');
            sb.append(nanos);
        }
    }

    static ZoneId toZoneId(final String zoneId) {
        return zoneId != null ? ZoneId.of(zoneId) : UTC;
    }

    static ChronoUnit toChronoUnit(final byte precision) {
        return switch (precision) {
            case AttributeConstants.Precisions.Timestamp.Minute -> ChronoUnit.MINUTES;
            case AttributeConstants.Precisions.Timestamp.Second -> ChronoUnit.SECONDS;
            case AttributeConstants.Precisions.Timestamp.Milli -> ChronoUnit.MILLIS;
            case AttributeConstants.Precisions.Timestamp.Micro -> ChronoUnit.MICROS;
            default -> ChronoUnit.NANOS;
        };
    }

    private static ZonedDateTime toZonedDateTime(final long nanos, final ZoneId zone) {
        return Instant.ofEpochSecond(nanos / NANOS_PER_SECOND, nanos % NANOS_PER_SECOND)
                .atZone(zone);
    }

    // nano-precision padding
    // formatting:off
    private static String[] pads = new String[] {
        "",
        "0",
        "00",
        "000",
        "0000",
        "00000",
        "000000",
        "0000000",
        "00000000",
        "000000000",
    };
    // formatting:on

    @SuppressWarnings("FinalParameters")
    private static int stringLength(long value) {
        final int negativeFactor = value < 0 ? 1 : 0;
        if (value < 0) value *= -1;
        if (value < 10) return 1 + negativeFactor;
        if (value < 100) return 2 + negativeFactor;
        if (value < 1000) return 3 + negativeFactor;
        if (value < 10000) return 4 + negativeFactor;
        if (value < 100000) return 5 + negativeFactor;
        if (value < 1000000) return 6 + negativeFactor;
        if (value < 10000000) return 7 + negativeFactor;
        if (value < 100000000) return 8 + negativeFactor;
        if (value < 1000000000L) return 9 + negativeFactor;
        if (value < 10000000000L) return 10 + negativeFactor;
        if (value < 100000000000L) return 11 + negativeFactor;
        if (value < 1000000000000L) return 12 + negativeFactor;
        if (value < 10000000000000L) return 13 + negativeFactor;
        if (value < 100000000000000L) return 14 + negativeFactor;
        if (value < 1000000000000000L) return 15 + negativeFactor;
        if (value < 10000000000000000L) return 16 + negativeFactor;
        if (value < 100000000000000000L) return 17 + negativeFactor;
        if (value < 1000000000000000000L) return 18 + negativeFactor;
        return 19 + negativeFactor;
    }
}
