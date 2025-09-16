// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.printer;

import static com.bytefacets.spinel.printer.RendererRegistry.registerDefault;
import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.schema.AttributeConstants;
import com.bytefacets.spinel.schema.ByteField;
import com.bytefacets.spinel.schema.Field;
import com.bytefacets.spinel.schema.TypeId;

/**
 * Registers byte renderers for Natural, Id, Quantity, Text, and Flag content types.
 *
 * <table>
 *     <tr>
 *         <th>ContentType</th>
 *         <th>Relevant Attributes</th>
 *         <th>Description</th>
 *     </tr>
 *     <tr>
 *         <td>AttributeConstants.ContentTypes.Natural</td>
 *         <td>none</td>
 *         <td>Will append the byte value to the StringBuilder as an int</td>
 *     </tr>
 *     <tr>
 *         <td>AttributeConstants.ContentTypes.Id</td>
 *         <td>none</td>
 *         <td>Will append the byte value to the StringBuilder as an int</td>
 *     </tr>
 *     <tr>
 *         <td>AttributeConstants.ContentTypes.Quantity</td>
 *         <td>none</td>
 *         <td>Will append the byte value to the StringBuilder as an int</td>
 *     </tr>
 *     <tr>
 *         <td>AttributeConstants.ContentTypes.Text</td>
 *         <td>none</td>
 *         <td>Will append the byte value to the StringBuilder as a char</td>
 *     </tr>
 *     <tr>
 *         <td>AttributeConstants.ContentTypes.Flag</td>
 *         <td>none</td>
 *         <td>Will append the byte value to the StringBuilder as a set of flags, e.g. 15 = {0,1,2,3}
 *         indicating the bits that are set</td>
 *     </tr>
 * </table>
 */
final class ByteRenderers {
    private static final RenderMethod NATURAL = StringBuilder::append;
    private static final RenderMethod FLAG = new FlagMethod();
    private static final RenderMethod TEXT = (sb, value) -> sb.append((char) value);

    private ByteRenderers() {}

    static void register() {
        registerDefault(
                TypeId.Byte,
                AttributeConstants.ContentTypes.Natural,
                sField -> new ByteRenderer(sField.field(), NATURAL));
        registerDefault(
                TypeId.Byte,
                AttributeConstants.ContentTypes.Id,
                sField -> new ByteRenderer(sField.field(), NATURAL));
        registerDefault(
                TypeId.Byte,
                AttributeConstants.ContentTypes.Quantity,
                sField -> new ByteRenderer(sField.field(), NATURAL));
        registerDefault(
                TypeId.Byte,
                AttributeConstants.ContentTypes.Text,
                sField -> new ByteRenderer(sField.field(), TEXT));
        registerDefault(
                TypeId.Byte,
                AttributeConstants.ContentTypes.Flag,
                sField -> new ByteRenderer(sField.field(), FLAG));
    }

    private static final class ByteRenderer implements ValueRenderer {
        private final ByteField field;
        private final RenderMethod renderMethod;

        private ByteRenderer(final Field field, final RenderMethod renderMethod) {
            this.field = (ByteField) requireNonNull(field, "field");
            this.renderMethod = requireNonNull(renderMethod, "renderMethod");
        }

        @Override
        public void render(final StringBuilder sb, final int row) {
            renderMethod.renderValue(sb, field.valueAt(row));
        }
    }

    private static final class FlagMethod implements RenderMethod {
        @Override
        public void renderValue(final StringBuilder sb, final byte value) {
            if (value == 0) {
                sb.append("{}");
                return;
            }
            sb.append('{');
            for (int i = 0; i < 8; i++) {
                if ((value & (1 << i)) != 0) {
                    sb.append(i).append(',');
                }
            }
            sb.setCharAt(sb.length() - 1, '}');
        }
    }

    interface RenderMethod {
        void renderValue(StringBuilder sb, byte value);
    }
}
