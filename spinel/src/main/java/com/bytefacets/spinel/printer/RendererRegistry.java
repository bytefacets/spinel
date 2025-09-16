// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.printer;

import com.bytefacets.collections.hash.ShortGenericIndexedMap;
import com.bytefacets.collections.types.Pack;
import com.bytefacets.spinel.schema.AttributeConstants;
import com.bytefacets.spinel.schema.BoolField;
import com.bytefacets.spinel.schema.ByteField;
import com.bytefacets.spinel.schema.CharField;
import com.bytefacets.spinel.schema.DoubleField;
import com.bytefacets.spinel.schema.Field;
import com.bytefacets.spinel.schema.FloatField;
import com.bytefacets.spinel.schema.IntField;
import com.bytefacets.spinel.schema.LongField;
import com.bytefacets.spinel.schema.SchemaField;
import com.bytefacets.spinel.schema.ShortField;
import com.bytefacets.spinel.schema.TypeId;
import java.util.function.Function;

/**
 * A registry of typed value renderers that use the Metadata on SchemaFields to pick to render
 * values in loggers and UIs.
 *
 * @see AttributeConstants
 */
public final class RendererRegistry {
    private static final ShortGenericIndexedMap<Function<SchemaField, ValueRenderer>> DEFAULTS =
            new ShortGenericIndexedMap<>(16);
    private static final ShortGenericIndexedMap<Function<SchemaField, ValueRenderer>> registry =
            new ShortGenericIndexedMap<>(16);

    /** Creates a new instance of the registry with a copy of the default renderer mappings. */
    public static RendererRegistry rendererRegistry() {
        return new RendererRegistry();
    }

    RendererRegistry() {
        registry.copyFrom(DEFAULTS);
    }

    /** A renderer bound to the given schema field. */
    public ValueRenderer renderer(final SchemaField field) {
        final byte content = AttributeConstants.contentType(field.metadata());
        final var match = registry.getOrDefault(Pack.packToShort(field.typeId(), content), null);
        if (match != null) {
            return match.apply(field);
        }
        final var defaultForType =
                registry.getOrDefault(
                        Pack.packToShort(field.typeId(), AttributeConstants.ContentTypes.Natural),
                        null);
        return defaultForType != null ? defaultForType.apply(field) : defaultRenderer(field);
    }

    /**
     * Registers a default renderer that will go to every subsequent instance of a RendererRegistry.
     */
    public static void registerDefault(
            final byte fieldType,
            final byte contentType,
            final Function<SchemaField, ValueRenderer> rendererSupplier) {
        DEFAULTS.put(Pack.packToShort(fieldType, contentType), rendererSupplier);
    }

    /** Registers a renderer mapping for this RendererRegistry instance. */
    public void register(
            final byte fieldType,
            final byte contentType,
            final Function<SchemaField, ValueRenderer> rendererSupplier) {
        registry.put(Pack.packToShort(fieldType, contentType), rendererSupplier);
    }

    private static ValueRenderer defaultRenderer(final SchemaField field) {
        final Field f = field.field();
        return switch (f.typeId()) {
            case TypeId.Bool -> (sb, row) -> sb.append(((BoolField) f).valueAt(row));
            case TypeId.Byte -> (sb, row) -> sb.append(((ByteField) f).valueAt(row));
            case TypeId.Short -> (sb, row) -> sb.append(((ShortField) f).valueAt(row));
            case TypeId.Char -> (sb, row) -> sb.append(((CharField) f).valueAt(row));
            case TypeId.Int -> (sb, row) -> sb.append(((IntField) f).valueAt(row));
            case TypeId.Long -> (sb, row) -> sb.append(((LongField) f).valueAt(row));
            case TypeId.Float -> (sb, row) -> sb.append(((FloatField) f).valueAt(row));
            case TypeId.Double -> (sb, row) -> sb.append(((DoubleField) f).valueAt(row));
            default -> (sb, row) -> sb.append(f.objectValueAt(row));
        };
    }
}
