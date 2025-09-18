// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.printer;

import static com.bytefacets.spinel.printer.KmbtRenderState.kmbFormat;
import static com.bytefacets.spinel.printer.RendererRegistry.registerDefault;
import static com.bytefacets.spinel.printer.TimeRenderers.durationRenderer;
import static com.bytefacets.spinel.printer.TimeRenderers.timeRenderer;
import static com.bytefacets.spinel.printer.TimeRenderers.timestampRenderer;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.types.LongType;
import com.bytefacets.collections.types.Pack;
import com.bytefacets.spinel.schema.AttributeConstants;
import com.bytefacets.spinel.schema.Field;
import com.bytefacets.spinel.schema.LongField;
import com.bytefacets.spinel.schema.Metadata;
import com.bytefacets.spinel.schema.TypeId;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Objects;

/**
 * Registers long renderers for Natural, Id, Duration, Time, Timestamp, Packed2, Packed4, Text, and
 * Flag content types.
 *
 * <table>
 *     <tr>
 *         <th>ContentType</th>
 *         <th>Relevant Attributes</th>
 *         <th>Description</th>
 *         <th>Additional Info</th>
 *     </tr>
 *     <tr>
 *         <td>AttributeConstants.ContentTypes.Natural</td>
 *         <td>DisplayFormat</td>
 *         <td>Will use DisplayFormat as override, or will build a format using DisplayPrecision</td>
 *         <td>When DisplayFormat is kMBT, values rendered in thousands, millions, billions, trillions</td>
 *     </tr>
 *     <tr>
 *         <td>AttributeConstants.ContentTypes.Id</td>
 *         <td>none</td>
 *         <td>Will use DisplayFormat as override if present</td>
 *         <td></td>
 *     </tr>
 *     <tr>
 *         <td>AttributeConstants.ContentTypes.Time</td>
 *         <td>DisplayPrecision, ValuePrecision, TimeZone</td>
 *         <td>Renders a time to nano precision</td>
 *         <td>ValuePrecision allows for values in minutes to nanos; Note that it goes thru Instant and ZonedDateTime</td>
 *     </tr>
 *     <tr>
 *         <td>AttributeConstants.ContentTypes.Timestamp</td>
 *         <td>DisplayPrecision, ValuePrecision, TimeZone</td>
 *         <td>Renders a timestamp to nano precision</td>
 *         <td>ValuePrecision allows for values in minutes to nanos; Note that it goes thru Instant and ZonedDateTime</td>
 *     </tr>
 *     <tr>
 *         <td>AttributeConstants.ContentTypes.Duration</td>
 *         <td>DisplayPrecision, ValuePrecision</td>
 *         <td>Renders a duration to nano precision</td>
 *         <td>ValuePrecision allows for values in minutes to nanos; Long.MIN_VALUE and MAX_VALUE
 *         are treated as no value and will not append anything</td>
 *     </tr>
 *     <tr>
 *         <td>AttributeConstants.ContentTypes.Text</td>
 *         <td>DisplayFormat (LittleEndian or BigEndian)</td>
 *         <td>Renders a string from the 8 bytes of the long value</td>
 *         <td>Default format is LittleEndian</td>
 *     </tr>
 *     <tr>
 *         <td>AttributeConstants.ContentTypes.Flag</td>
 *         <td>none</td>
 *         <td>Will append the long value to the StringBuilder as a set of flags, e.g. 15 = {0,1,2,3}
 *         indicating the bits that are set</td>
 *         <td></td>
 *     </tr>
 *     <tr>
 *         <td>AttributeConstants.ContentTypes.Pack2</td>
 *         <td>none</td>
 *         <td>Will append the the long as a tuple of two ints, e.g. (2762,3873)</td>
 *         <td></td>
 *     </tr>
 *     <tr>
 *         <td>AttributeConstants.ContentTypes.Pack4</td>
 *         <td>none</td>
 *         <td>Will append the the long as a tuple of four shorts, e.g. (10,28,76,89)</td>
 *         <td></td>
 *     </tr>
 * </table>
 *
 * @see LongType#readLE(byte[], int)
 * @see LongType#readBE(byte[], int)
 */
final class LongRenderers {
    private static final RenderMethod FLAG = new FlagMethod();
    private static final RenderMethod PACK2 = new Pack2Method();
    private static final RenderMethod PACK4 = new Pack4Method();

    private LongRenderers() {}

    static void register() {
        registerDefault(
                TypeId.Long,
                AttributeConstants.ContentTypes.Natural,
                sField -> new LongRenderer(sField.field(), new NaturalRenderer(sField.metadata())));
        registerDefault(
                TypeId.Long,
                AttributeConstants.ContentTypes.Id,
                sField -> new LongRenderer(sField.field(), StringBuilder::append));
        registerDefault(
                TypeId.Long,
                AttributeConstants.ContentTypes.Text,
                sField -> new LongRenderer(sField.field(), new TextRender(sField.metadata())));
        registerDefault(
                TypeId.Long,
                AttributeConstants.ContentTypes.Time,
                sField -> new LongRenderer(sField.field(), timeRenderer(sField.metadata())));
        registerDefault(
                TypeId.Long,
                AttributeConstants.ContentTypes.Timestamp,
                sField -> new LongRenderer(sField.field(), timestampRenderer(sField.metadata())));
        registerDefault(
                TypeId.Long,
                AttributeConstants.ContentTypes.Duration,
                sField -> new LongRenderer(sField.field(), durationRenderer(sField.metadata())));
        registerDefault(
                TypeId.Long,
                AttributeConstants.ContentTypes.Flag,
                sField -> new LongRenderer(sField.field(), FLAG));
        registerDefault(
                TypeId.Long,
                AttributeConstants.ContentTypes.Packed2,
                sField -> new LongRenderer(sField.field(), PACK2));
        registerDefault(
                TypeId.Long,
                AttributeConstants.ContentTypes.Packed4,
                sField -> new LongRenderer(sField.field(), PACK4));
    }

    private static final class NaturalRenderer implements RenderMethod {
        private final NumberFormat format;

        private NaturalRenderer(final Metadata metadata) {
            final String formatStr = AttributeConstants.displayFormat(metadata);
            if (formatStr != null) {
                format = formatter(metadata);
            } else {
                format = null;
            }
        }

        @Override
        public void renderValue(final StringBuilder sb, final long value) {
            if (format == null) {
                sb.append(value);
            } else {
                sb.append(format.format(value));
            }
        }
    }

    private static final class TextRender implements RenderMethod {
        private final byte[] data = new byte[8];
        private final boolean little;

        private TextRender(final Metadata metadata) {
            final String format = AttributeConstants.displayFormat(metadata);
            little = !Objects.equals(format, AttributeConstants.DisplayFormats.BigEndian);
        }

        private void renderLE(final StringBuilder sb, final long value) {
            LongType.writeLE(data, 0, value);
            for (int i = 0; i < 8; i++) {
                sb.append((char) data[i]);
            }
        }

        private void renderBE(final StringBuilder sb, final long value) {
            LongType.writeBE(data, 0, value);
            for (int i = 0; i < 8; i++) {
                sb.append((char) data[i]);
            }
        }

        @Override
        public void renderValue(final StringBuilder sb, final long value) {
            if (little) {
                renderLE(sb, value);
            } else {
                renderBE(sb, value);
            }
        }
    }

    private static NumberFormat formatter(final Metadata metadata) {
        String format = AttributeConstants.displayFormat(metadata);
        if (Objects.equals(format, AttributeConstants.DisplayFormats.kMBT)) {
            format = "#,##0";
            return kmbFormat(new DecimalFormat(format));
        } else {
            return new DecimalFormat("#,##0");
        }
    }

    interface RenderMethod {
        void renderValue(StringBuilder sb, long value);
    }

    private static final class LongRenderer implements ValueRenderer {
        private final RenderMethod renderMethod;
        private final LongField field;

        private LongRenderer(final Field field, final RenderMethod renderMethod) {
            this.field = (LongField) requireNonNull(field, "field");
            this.renderMethod = requireNonNull(renderMethod, "renderMethod");
        }

        @Override
        public void render(final StringBuilder sb, final int row) {
            renderMethod.renderValue(sb, field.valueAt(row));
        }
    }

    private static final class FlagMethod implements RenderMethod {
        @Override
        public void renderValue(final StringBuilder sb, final long value) {
            if (value == 0) {
                sb.append("{}");
                return;
            }
            sb.append('{');
            final int start = Long.numberOfTrailingZeros(value); // lowest set bit
            final int end = Long.SIZE - 1 - Long.numberOfLeadingZeros(value); // highest set bit
            for (int i = start; i <= end; i++) {
                if ((value & (1L << i)) != 0) {
                    sb.append(i).append(',');
                }
            }
            sb.setCharAt(sb.length() - 1, '}');
        }
    }

    private static final class Pack2Method implements RenderMethod {
        @Override
        public void renderValue(final StringBuilder sb, final long value) {
            sb.append('(')
                    .append(Pack.unpackHiInt(value))
                    .append(',')
                    .append(Pack.unpackLoInt(value))
                    .append(')');
        }
    }

    private static final class Pack4Method implements RenderMethod {
        @Override
        public void renderValue(final StringBuilder sb, final long value) {
            final int hi = Pack.unpackHiInt(value);
            final int lo = Pack.unpackLoInt(value);
            // formatting:off
            sb.append('(')
                    .append(Pack.unpackHiShort(hi)).append(',')
                    .append(Pack.unpackLoShort(hi)).append(',')
                    .append(Pack.unpackHiShort(lo)).append(',')
                    .append(Pack.unpackLoShort(lo)).append(')');
            // formatting:on
        }
    }
}
