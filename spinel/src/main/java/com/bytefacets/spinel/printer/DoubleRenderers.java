// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.printer;

import static com.bytefacets.spinel.printer.KmbtRenderState.kmbFormat;
import static com.bytefacets.spinel.printer.RendererRegistry.registerDefault;
import static com.bytefacets.spinel.schema.AttributeConstants.DisplayFormats;
import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.schema.AttributeConstants;
import com.bytefacets.spinel.schema.DoubleField;
import com.bytefacets.spinel.schema.Field;
import com.bytefacets.spinel.schema.Metadata;
import com.bytefacets.spinel.schema.TypeId;
import java.text.DecimalFormat;
import java.text.NumberFormat;

/**
 * Registers double renderers for Natural, Quantity, and Percent.
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
 *         <td>DisplayPrecision, DisplayFormat</td>
 *         <td>Will use DisplayFormat as override, or will build a format using DisplayPrecision</td>
 *         <td>When DisplayFormat is kMBT, values rendered in thousands, millions, billions, trillions</td>
 *     </tr>
 *     <tr>
 *         <td>AttributeConstants.ContentTypes.Quantity</td>
 *         <td>DisplayPrecision, DisplayFormat</td>
 *         <td>Will use DisplayFormat as override, or will build a format using DisplayPrecision</td>
 *         <td>When DisplayFormat is kMBT, values rendered in thousands, millions, billions, trillions</td>
 *     </tr>
 *     <tr>
 *         <td>AttributeConstants.ContentTypes.Percent</td>
 *         <td>DisplayPrecision, DisplayFormat</td>
 *         <td>Will use DisplayFormat as override, or will build a percent format using DisplayPrecision</td>
 *         <td>Values will show as (value * 100) as is customary with percent formats</td>
 *     </tr>
 * </table>
 */
@SuppressWarnings("NeedBraces")
final class DoubleRenderers {
    private DoubleRenderers() {}

    static void register() {
        registerDefault(
                TypeId.Double,
                AttributeConstants.ContentTypes.Natural,
                sField -> new DoubleRenderer(sField.field(), formatter(sField.metadata())));
        registerDefault(
                TypeId.Double,
                AttributeConstants.ContentTypes.Quantity,
                sField -> new DoubleRenderer(sField.field(), formatter(sField.metadata())));
        registerDefault(
                TypeId.Double,
                AttributeConstants.ContentTypes.Percent,
                sField -> new DoubleRenderer(sField.field(), percentFormatter(sField.metadata())));
    }

    private static final class DoubleRenderer implements ValueRenderer {
        private final NumberFormat formatter;
        private final DoubleField field;

        private DoubleRenderer(final Field field, final NumberFormat formatter) {
            this.field = (DoubleField) requireNonNull(field, "field");
            this.formatter = requireNonNull(formatter, "formatter");
        }

        @Override
        public void render(final StringBuilder sb, final int row) {
            sb.append(formatter.format(field.valueAt(row)));
        }
    }

    private static NumberFormat formatter(final Metadata metadata) {
        final int displayPrecision = AttributeConstants.displayPrecision(metadata, (byte) 2);
        String format = AttributeConstants.displayFormat(metadata);
        if (format == null) {
            if (displayPrecision < 0) return NumberFormat.getInstance();
            format = "#,##0";
            if (displayPrecision > 0) format += "." + "0".repeat(displayPrecision);
        }
        if (format.equalsIgnoreCase(DisplayFormats.kMBT)) {
            format = "#,##0";
            if (displayPrecision > 0) format += "." + "0".repeat(displayPrecision);
            return kmbFormat(new DecimalFormat(format));
        } else {
            return new DecimalFormat(format);
        }
    }

    private static NumberFormat percentFormatter(final Metadata metadata) {
        final int displayPrecision = AttributeConstants.displayPrecision(metadata, (byte) 2);
        String format = AttributeConstants.displayFormat(metadata);
        if (format == null) {
            if (displayPrecision < 0) return NumberFormat.getPercentInstance();
            format = "#,##0";
            if (displayPrecision > 0) format += "." + "0".repeat(displayPrecision);
            format += "%";
        }
        return new DecimalFormat(format);
    }
}
