// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.printer;

import static com.bytefacets.spinel.printer.KmbtRenderState.kmbFormat;
import static com.bytefacets.spinel.printer.RendererRegistry.registerDefault;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.types.Pack;
import com.bytefacets.collections.types.ShortType;
import com.bytefacets.spinel.schema.AttributeConstants;
import com.bytefacets.spinel.schema.Field;
import com.bytefacets.spinel.schema.Metadata;
import com.bytefacets.spinel.schema.ShortField;
import com.bytefacets.spinel.schema.TypeId;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Objects;

/**
 * Registers short renderers for Natural, Id, Quantity, Text, and Flag content types.
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
 *         <td>none</td>
 *         <td>Will append the short value to the StringBuilder</td>
 *         <td></td>
 *     </tr>
 *     <tr>
 *         <td>AttributeConstants.ContentTypes.Id</td>
 *         <td>none</td>
 *         <td>Will append the short value to the StringBuilder</td>
 *         <td></td>
 *     </tr>
 *     <tr>
 *         <td>AttributeConstants.ContentTypes.Quantity</td>
 *         <td>DisplayFormat</td>
 *         <td>Will use DisplayFormat as override, or will build a format using DisplayPrecision</td>
 *         <td>When DisplayFormat is kMBT, values rendered in thousands, millions, billions, trillions</td>
 *     </tr>
 *     <tr>
 *         <td>AttributeConstants.ContentTypes.Text</td>
 *         <td>DisplayFormat (LittleEndian or BigEndian)</td>
 *         <td>Renders a string from the 2 bytes of the short value</td>
 *         <td>Default format is LittleEndian</td>
 *     </tr>
 *     <tr>
 *         <td>AttributeConstants.ContentTypes.Flag</td>
 *         <td>none</td>
 *         <td>Will append the short value to the StringBuilder as a set of flags, e.g. 15 = {0,1,2,3}
 *         indicating the bits that are set</td>
 *         <td></td>
 *     </tr>
 *     <tr>
 *         <td>AttributeConstants.ContentTypes.Packed2</td>
 *         <td>none</td>
 *         <td>Will append the the short as a tuple of two bytes, e.g. (45,67)</td>
 *         <td></td>
 *     </tr>
 * </table>
 *
 * @see ShortType#writeBE(byte[], int, short)
 * @see ShortType#writeLE(byte[], int, short)
 */
final class ShortRenderers {
    private static final RenderMethod NATURAL = StringBuilder::append;
    private static final RenderMethod FLAG = new FlagMethod();
    private static final RenderMethod PACK2 = new Pack2Method();

    private ShortRenderers() {}

    static void register() {
        registerDefault(
                TypeId.Short,
                AttributeConstants.ContentTypes.Natural,
                sField -> new ShortRenderer(sField.field(), NATURAL));
        registerDefault(
                TypeId.Short,
                AttributeConstants.ContentTypes.Id,
                sField -> new ShortRenderer(sField.field(), NATURAL));
        registerDefault(
                TypeId.Short,
                AttributeConstants.ContentTypes.Quantity,
                sField ->
                        new ShortRenderer(
                                sField.field(), formatMethod(quantity(sField.metadata()))));
        registerDefault(
                TypeId.Short,
                AttributeConstants.ContentTypes.Text,
                sField -> new ShortRenderer(sField.field(), new TextRender(sField.metadata())));
        registerDefault(
                TypeId.Short,
                AttributeConstants.ContentTypes.Flag,
                sField -> new ShortRenderer(sField.field(), FLAG));
        registerDefault(
                TypeId.Short,
                AttributeConstants.ContentTypes.Packed2,
                sField -> new ShortRenderer(sField.field(), PACK2));
    }

    interface RenderMethod {
        void renderValue(StringBuilder sb, short value);
    }

    private static final class ShortRenderer implements ValueRenderer {
        private final ShortField field;
        private final RenderMethod method;

        private ShortRenderer(final Field field, final RenderMethod method) {
            this.field = (ShortField) requireNonNull(field, "field");
            this.method = requireNonNull(method, "method");
        }

        @Override
        public void render(final StringBuilder sb, final int row) {
            method.renderValue(sb, field.valueAt(row));
        }
    }

    static RenderMethod formatMethod(final NumberFormat fmt) {
        return (sb, value) -> sb.append(fmt.format(value));
    }

    static NumberFormat quantity(final Metadata metadata) {
        final String format = AttributeConstants.displayFormat(metadata);
        if (format == null) {
            return NumberFormat.getInstance();
        } else {
            if (format.equals(AttributeConstants.DisplayFormats.kMBT)) {
                return kmbFormat(new DecimalFormat("#,###"));
            } else {
                return new DecimalFormat(format);
            }
        }
    }

    private static final class TextRender implements RenderMethod {
        private final byte[] data = new byte[2];
        private final boolean little;

        private TextRender(final Metadata metadata) {
            final String format = AttributeConstants.displayFormat(metadata);
            little = !Objects.equals(format, AttributeConstants.DisplayFormats.BigEndian);
        }

        private void renderLE(final StringBuilder sb, final short value) {
            ShortType.writeLE(data, 0, value);
            for (int i = 0; i < 2; i++) {
                sb.append((char) data[i]);
            }
        }

        private void renderBE(final StringBuilder sb, final short value) {
            ShortType.writeBE(data, 0, value);
            for (int i = 0; i < 2; i++) {
                sb.append((char) data[i]);
            }
        }

        @Override
        public void renderValue(final StringBuilder sb, final short value) {
            if (little) {
                renderLE(sb, value);
            } else {
                renderBE(sb, value);
            }
        }
    }

    private static final class FlagMethod implements RenderMethod {
        @Override
        public void renderValue(final StringBuilder sb, final short value) {
            if (value == 0) {
                sb.append("{}");
                return;
            }
            sb.append('{');
            final int start = Integer.numberOfTrailingZeros(value);
            // shift so top of short is at top of int
            final int end = 15 - Integer.numberOfLeadingZeros(value << 16);
            for (int i = start; i <= end; i++) {
                if ((value & (1 << i)) != 0) {
                    sb.append(i).append(',');
                }
            }
            sb.setCharAt(sb.length() - 1, '}');
        }
    }

    private static final class Pack2Method implements RenderMethod {
        @Override
        public void renderValue(final StringBuilder sb, final short value) {
            sb.append('(')
                    .append(Pack.unpackHiByte(value))
                    .append(',')
                    .append(Pack.unpackLoByte(value))
                    .append(')');
        }
    }
}
