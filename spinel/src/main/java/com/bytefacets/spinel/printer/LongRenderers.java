// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.printer;

import static com.bytefacets.spinel.printer.KmbtRenderState.kmbFormat;
import static com.bytefacets.spinel.printer.RendererRegistry.registerDefault;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.types.LongType;
import com.bytefacets.spinel.schema.AttributeConstants;
import com.bytefacets.spinel.schema.Field;
import com.bytefacets.spinel.schema.LongField;
import com.bytefacets.spinel.schema.Metadata;
import com.bytefacets.spinel.schema.TypeId;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Objects;

/**
 * Registers long renderers for Natural, Id, and Text content types.
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
 *         <td>AttributeConstants.ContentTypes.Text</td>
 *         <td>DisplayFormat (LittleEndian or BigEndian)</td>
 *         <td>Renders a string from the 8 bytes of the long value</td>
 *         <td>Default format is LittleEndian</td>
 *     </tr>
 * </table>
 *
 * @see LongType#readLE(byte[], int)
 * @see LongType#readBE(byte[], int)
 */
final class LongRenderers {

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
}
