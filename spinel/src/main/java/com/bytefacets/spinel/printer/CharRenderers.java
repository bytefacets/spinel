// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.printer;

import static com.bytefacets.spinel.printer.KmbtRenderState.kmbFormat;
import static com.bytefacets.spinel.printer.RendererRegistry.registerDefault;
import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.schema.AttributeConstants;
import com.bytefacets.spinel.schema.CharField;
import com.bytefacets.spinel.schema.Field;
import com.bytefacets.spinel.schema.Metadata;
import com.bytefacets.spinel.schema.TypeId;
import java.text.DecimalFormat;
import java.text.NumberFormat;

/**
 * Registers char renderers for Natural, Id, and Quantity content types.
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
 *         <td>Will append the char value to the StringBuilder</td>
 *         <td></td>
 *     <tr>
 *         <td>AttributeConstants.ContentTypes.Id</td>
 *         <td>none</td>
 *         <td>Will append the char value to the StringBuilder</td>
 *         <td></td>
 *     </tr>
 *     <tr>
 *         <td>AttributeConstants.ContentTypes.Quantity</td>
 *         <td>DisplayFormat</td>
 *         <td>Will use DisplayFormat as override, or will build a format using DisplayPrecision</td>
 *         <td>When DisplayFormat is kMBT, values rendered in thousands, millions, billions, trillions</td>
 *         <td></td>
 *     </tr>
 * </table>
 */
final class CharRenderers {
    private static final RenderMethod NATURAL = StringBuilder::append;

    private CharRenderers() {}

    static void register() {
        registerDefault(
                TypeId.Char,
                AttributeConstants.ContentTypes.Natural,
                sField -> new CharRenderer(sField.field(), NATURAL));
        registerDefault(
                TypeId.Char,
                AttributeConstants.ContentTypes.Id,
                sField -> new CharRenderer(sField.field(), NATURAL));
        registerDefault(
                TypeId.Char,
                AttributeConstants.ContentTypes.Quantity,
                sField ->
                        new CharRenderer(
                                sField.field(), formatMethod(quantity(sField.metadata()))));
    }

    interface RenderMethod {
        void renderValue(StringBuilder sb, char value);
    }

    private static final class CharRenderer implements ValueRenderer {
        private final CharField field;
        private final RenderMethod method;

        private CharRenderer(final Field field, final RenderMethod method) {
            this.field = (CharField) requireNonNull(field, "field");
            this.method = requireNonNull(method, "method");
        }

        @Override
        public void render(final StringBuilder sb, final int row) {
            method.renderValue(sb, field.valueAt(row));
        }
    }

    static RenderMethod formatMethod(final NumberFormat fmt) {
        return (sb, value) -> sb.append(fmt.format((int) value));
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
}
