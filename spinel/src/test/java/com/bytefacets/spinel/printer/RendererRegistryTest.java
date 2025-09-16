// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.printer;

import static com.bytefacets.spinel.printer.RendererRegistry.rendererRegistry;
import static com.bytefacets.spinel.schema.SchemaField.schemaField;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.bytefacets.collections.types.BoolType;
import com.bytefacets.collections.types.ByteType;
import com.bytefacets.collections.types.CharType;
import com.bytefacets.collections.types.DoubleType;
import com.bytefacets.collections.types.FloatType;
import com.bytefacets.collections.types.GenericType;
import com.bytefacets.collections.types.LongType;
import com.bytefacets.collections.types.ShortType;
import com.bytefacets.collections.types.StringType;
import com.bytefacets.spinel.schema.AttributeConstants;
import com.bytefacets.spinel.schema.BoolField;
import com.bytefacets.spinel.schema.ByteField;
import com.bytefacets.spinel.schema.CharField;
import com.bytefacets.spinel.schema.DoubleField;
import com.bytefacets.spinel.schema.Field;
import com.bytefacets.spinel.schema.FloatField;
import com.bytefacets.spinel.schema.GenericField;
import com.bytefacets.spinel.schema.IntField;
import com.bytefacets.spinel.schema.LongField;
import com.bytefacets.spinel.schema.Metadata;
import com.bytefacets.spinel.schema.SchemaField;
import com.bytefacets.spinel.schema.ShortField;
import com.bytefacets.spinel.schema.StringField;
import com.bytefacets.spinel.schema.TypeId;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class RendererRegistryTest {
    private final Map<String, Object> attrs = new HashMap<>();
    private final StringBuilder sb = new StringBuilder();
    private final RendererRegistry instance = rendererRegistry();

    @Test
    void shouldRegisterCustom() {
        AttributeConstants.setContentType(attrs, (byte) 56);
        // formatting:off
        instance.register(TypeId.Int, (byte) 56, sField -> (sb1, row) ->
                sb.append(String.format("Row[%d]=%s", row, sField.objectValueAt(row))));
        // formatting:on
        final var renderer = instance.renderer(field(toField(TypeId.Int, 567)));
        validate(renderer, "Row[6]=567");
    }

    private static Stream<Arguments> typeArgs() {
        return IntStream.rangeClosed(TypeId.Min, TypeId.Max)
                .mapToObj(typeId -> schemaField(0, "", toField((byte) typeId, 65)))
                .map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("typeArgs")
    void shouldHaveDefaultsForTypes(final SchemaField field) {
        final var renderer = instance.renderer(field);
        switch (field.typeId()) {
            case TypeId.Bool -> validate(renderer, "true");
            case TypeId.Char -> validate(renderer, "A");
            case TypeId.Float, TypeId.Double -> validate(renderer, "65.0");
            default -> validate(renderer, "65");
        }
    }

    private void validate(final ValueRenderer renderer, final String expected) {
        renderer.render(sb, 6);
        assertThat(sb.toString(), equalTo(expected));
    }

    final SchemaField field(final Field field) {
        return schemaField(0, "", field, Metadata.metadata(attrs));
    }

    static Field toField(final byte type, final int value) {
        return switch (type) {
            case TypeId.Bool -> (BoolField) (row -> BoolType.castToBool(value));
            case TypeId.Byte -> (ByteField) (row -> ByteType.castToByte(value));
            case TypeId.Short -> (ShortField) (row -> ShortType.castToShort(value));
            case TypeId.Char -> (CharField) (row -> CharType.castToChar(value));
            case TypeId.Int -> (IntField) (row -> value);
            case TypeId.Long -> (LongField) (row -> LongType.castToLong(value));
            case TypeId.Float -> (FloatField) (row -> FloatType.castToFloat(value));
            case TypeId.Double -> (DoubleField) (row -> DoubleType.castToDouble(value));
            case TypeId.String -> (StringField) (row -> StringType.castToString(value));
            default -> (GenericField) (row -> GenericType.castToGeneric(value));
        };
    }
}
