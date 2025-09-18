// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.facade;

import com.bytefacets.spinel.schema.AttributeConstants;
import com.bytefacets.spinel.schema.DisplayMetadata;
import com.bytefacets.spinel.schema.FieldDescriptor;
import com.bytefacets.spinel.schema.Metadata;
import com.bytefacets.spinel.schema.TypeId;
import com.bytefacets.spinel.schema.ValueMetadata;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public final class StructFieldExtractor {
    private StructFieldExtractor() {}

    public static void consumeFields(
            final Class<?> type,
            final Consumer<FieldDescriptor> writableFieldConsumer,
            final Consumer<FieldDescriptor> readableFieldConsumer) {
        final TypeInfo info = Inspector.typeInspector().inspect(type);
        final Collection<FieldInfo> fields = info.fields();
        fields.forEach(
                field -> {
                    final var attrs = readMetadataAttributes(field);
                    final var meta = attrs != null ? Metadata.metadata(attrs) : Metadata.EMPTY;
                    final var fd =
                            new FieldDescriptor(TypeId.toId(field.type()), field.getName(), meta);
                    if (field.isWritable()) {
                        writableFieldConsumer.accept(fd);
                    } else {
                        readableFieldConsumer.accept(fd);
                    }
                });
    }

    private static Map<String, Object> readMetadataAttributes(final FieldInfo info) {
        Map<String, Object> attrs = null;
        for (Method method : new Method[] {info.getterMethod(), info.setterMethod()}) {
            if (method == null) {
                continue;
            }
            for (var annotation : method.getDeclaredAnnotations()) {
                if (annotation instanceof ValueMetadata content) {
                    attrs = collectValueMetadata(content, attrs);
                } else if (annotation instanceof DisplayMetadata display) {
                    attrs = collectDisplayMetadata(display, attrs);
                }
            }
        }
        return attrs;
    }

    private static Map<String, Object> collectValueMetadata(
            final ValueMetadata content, final Map<String, Object> attrs) {
        final Byte contentType = resolveByte(content.contentType());
        final Byte precision = resolveByte(content.precision());
        Map<String, Object> result = attrs;
        if (contentType != null || precision != null) {
            if (result == null) {
                result = new HashMap<>(4);
            }
            putIfNonNull(result, AttributeConstants.ValuePrecision, precision);
            putIfNonNull(result, AttributeConstants.ContentType, contentType);
        }
        return result;
    }

    private static Map<String, Object> collectDisplayMetadata(
            final DisplayMetadata display, final Map<String, Object> attrs) {
        final String format = resolveString(display.format());
        final String zone = resolveString(display.zoneId());
        final Byte precision = resolveByte(display.precision());
        Map<String, Object> result = attrs;
        if (format != null || precision != null || zone != null) {
            if (result == null) {
                result = new HashMap<>(4);
            }
            putIfNonNull(result, AttributeConstants.DisplayPrecision, precision);
            putIfNonNull(result, AttributeConstants.DisplayFormat, format);
            putIfNonNull(result, AttributeConstants.TimeZone, zone);
        }
        return result;
    }

    private static void putIfNonNull(
            final Map<String, Object> map, final String key, final Object value) {
        if (value != null) {
            map.put(key, value);
        }
    }

    private static String resolveString(final String value) {
        return value != null && !value.isEmpty() ? value : null;
    }

    private static Byte resolveByte(final byte value) {
        return value != Byte.MIN_VALUE ? value : null;
    }
}
