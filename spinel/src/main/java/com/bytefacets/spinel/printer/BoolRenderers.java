// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.printer;

import static com.bytefacets.spinel.printer.RendererRegistry.registerDefault;
import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.schema.AttributeConstants;
import com.bytefacets.spinel.schema.BoolField;
import com.bytefacets.spinel.schema.Field;
import com.bytefacets.spinel.schema.TypeId;

/**
 * Registers boolean renderer for "Natural" content type.
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
 *         <td>Will append the boolean value to the StringBuilder</td>
 *         <td></td>
 *     </tr>
 * </table>
 */
@SuppressWarnings("NeedBraces")
final class BoolRenderers {
    private BoolRenderers() {}

    static void register() {
        registerDefault(
                TypeId.Bool,
                AttributeConstants.ContentTypes.Natural,
                sField -> new BoolRenderer(sField.field()));
    }

    private static final class BoolRenderer implements ValueRenderer {
        private final BoolField field;

        private BoolRenderer(final Field field) {
            this.field = (BoolField) requireNonNull(field, "field");
        }

        @Override
        public void render(final StringBuilder sb, final int row) {
            sb.append(field.valueAt(row));
        }
    }
}
