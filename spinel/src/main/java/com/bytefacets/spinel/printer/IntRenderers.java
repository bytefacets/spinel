// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.printer;

import static com.bytefacets.spinel.printer.KmbtRenderState.kmbFormat;
import static com.bytefacets.spinel.printer.RendererRegistry.registerDefault;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.types.IntType;
import com.bytefacets.spinel.schema.AttributeConstants;
import com.bytefacets.spinel.schema.Field;
import com.bytefacets.spinel.schema.IntField;
import com.bytefacets.spinel.schema.Metadata;
import com.bytefacets.spinel.schema.TypeId;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Objects;

/**
 * Registers int renderers for Natural, Id, Quantity, Text, and Flag content types.
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
 *         <td>Will append the int value to the StringBuilder</td>
 *         <td></td>
 *     </tr>
 *     <tr>
 *         <td>AttributeConstants.ContentTypes.Id</td>
 *         <td>none</td>
 *         <td>Will append the int value to the StringBuilder</td>
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
 *         <td>Renders a string from the 4 bytes of the int value</td>
 *         <td>Default format is LittleEndian</td>
 *     </tr>
 *     <tr>
 *         <td>AttributeConstants.ContentTypes.Flag</td>
 *         <td>none</td>
 *         <td>Will append the int value to the StringBuilder as a set of flags, e.g. 15 = {0,1,2,3}
 *         indicating the bits that are set</td>
 *         <td></td>
 *     </tr>
 * </table>
 *
 * @see IntType#writeBE(byte[], int, int)
 * @see IntType#writeLE(byte[], int, int)
 */
final class IntRenderers {
    private static final RenderMethod NATURAL = StringBuilder::append;
    private static final RenderMethod FLAG = new FlagMethod();

    private IntRenderers() {}

    static void register() {
        registerDefault(
                TypeId.Int,
                AttributeConstants.ContentTypes.Natural,
                sField -> new IntRenderer(sField.field(), NATURAL));
        registerDefault(
                TypeId.Int,
                AttributeConstants.ContentTypes.Id,
                sField -> new IntRenderer(sField.field(), NATURAL));
        registerDefault(
                TypeId.Int,
                AttributeConstants.ContentTypes.Quantity,
                sField ->
                        new IntRenderer(sField.field(), formatMethod(quantity(sField.metadata()))));
        registerDefault(
                TypeId.Int,
                AttributeConstants.ContentTypes.Text,
                sField -> new IntRenderer(sField.field(), new TextRender(sField.metadata())));
        registerDefault(
                TypeId.Int,
                AttributeConstants.ContentTypes.Flag,
                sField -> new IntRenderer(sField.field(), FLAG));
    }

    interface RenderMethod {
        void renderValue(StringBuilder sb, int value);
    }

    private static final class IntRenderer implements ValueRenderer {
        private final IntField field;
        private final RenderMethod method;

        private IntRenderer(final Field field, final RenderMethod method) {
            this.field = (IntField) requireNonNull(field, "field");
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
        private final byte[] data = new byte[4];
        private final boolean little;

        private TextRender(final Metadata metadata) {
            final String format = AttributeConstants.displayFormat(metadata);
            little = !Objects.equals(format, AttributeConstants.DisplayFormats.BigEndian);
        }

        private void renderLE(final StringBuilder sb, final int value) {
            IntType.writeLE(data, 0, value);
            for (int i = 0; i < 4; i++) {
                sb.append((char) data[i]);
            }
        }

        private void renderBE(final StringBuilder sb, final int value) {
            IntType.writeBE(data, 0, value);
            for (int i = 0; i < 4; i++) {
                sb.append((char) data[i]);
            }
        }

        @Override
        public void renderValue(final StringBuilder sb, final int value) {
            if (little) {
                renderLE(sb, value);
            } else {
                renderBE(sb, value);
            }
        }
    }

    private static final class FlagMethod implements RenderMethod {
        @Override
        public void renderValue(final StringBuilder sb, final int value) {
            if (value == 0) {
                sb.append("{}");
                return;
            }
            sb.append('{');
            // lowest set bit
            final int start = Integer.numberOfTrailingZeros(value);
            // highest set bit
            final int end = Integer.SIZE - 1 - Integer.numberOfLeadingZeros(value);
            for (int i = start; i <= end; i++) {
                if ((value & (1 << i)) != 0) {
                    sb.append(i).append(',');
                }
            }
            sb.setCharAt(sb.length() - 1, '}');
        }
    }
}
