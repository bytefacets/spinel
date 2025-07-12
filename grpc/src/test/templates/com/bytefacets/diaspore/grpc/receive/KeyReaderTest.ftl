<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.grpc.receive;
<#if type.name == "Int" || type.name == "Char" || type.name == "Short">
    <#assign readerType="Int32">
<#elseif type.name == "Long">
    <#assign readerType="Int64">
<#else>
    <#assign readerType="${type.name}">
</#if>
import static com.bytefacets.diaspore.schema.ArrayFieldFactory.writable${type.name}ArrayField;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

<#if type.name == "Generic">
import com.bytefacets.diaspore.grpc.send.ObjectEncoderAccess;
</#if>
import com.bytefacets.collections.hash.StringGenericIndexedMap;
import com.bytefacets.collections.types.${type.name}Type;
import com.bytefacets.diaspore.grpc.proto.${readerType}Data;
import com.bytefacets.diaspore.grpc.proto.DataUpdate;
import com.bytefacets.diaspore.grpc.proto.ResponseType;
import com.bytefacets.diaspore.schema.${type.name}WritableField;
import com.bytefacets.diaspore.schema.FieldChangeListener;
import com.bytefacets.diaspore.schema.FieldList;
import com.bytefacets.diaspore.schema.SchemaField;
import java.util.BitSet;
import java.util.List;
import java.util.stream.IntStream;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ${type.name}ReaderTest {
    private final ${readerType}Reader reader = new ${readerType}Reader();
    private final BitSet changedFields = new BitSet();
    private @Mock FieldChangeListener fieldListener;
    private ${type.name}WritableField field0;
    private ${type.name}WritableField field1;
    private DataUpdate data;
<#if type.name == "Generic">
    private final ObjectEncoderAccess encoder = new ObjectEncoderAccess();
</#if>

    @BeforeEach
    void setUp() {
        field0 = writable${type.name}ArrayField(4, 0, fieldListener);
        field1 = writable${type.name}ArrayField(4, 1, fieldListener);
        final var fmap = new StringGenericIndexedMap<SchemaField>(4);
        fmap.put("f0", SchemaField.schemaField(0, "f0", field0));
        fmap.put("f1", SchemaField.schemaField(1, "f1", field1));
        reader.setContext(FieldList.fieldList(fmap), changedFields);
        data =
                DataUpdate.newBuilder()
                        .add${readerType}Data(data(1, 1, 0, 7, 3, 8))
                        .add${readerType}Data(data(0, 7, 6, 0, 2, 9))
                        .addAllRows(List.of(1, 3, 6, 7, 10))
                        .build();
    }

    @ParameterizedTest
    @EnumSource(
            value = ResponseType.class,
            names = {"RESPONSE_TYPE_ADD", "RESPONSE_TYPE_CHG"})
    void shouldReadValuesIntoField(final ResponseType type) {
        reader.read(data, type);
        assertThat(
                readFieldValues(field1, data.getRowsList()),
                contains(v(1), v(0), v(7), v(3), v(8)));
        assertThat(
                readFieldValues(field0, data.getRowsList()),
                contains(v(7), v(6), v(0), v(2), v(9)));
    }

    @ParameterizedTest
    @EnumSource(
            value = ResponseType.class,
            names = {"RESPONSE_TYPE_ADD", "RESPONSE_TYPE_CHG", "RESPONSE_TYPE_REM"})
    void shouldSetChangedFieldsOnlyOnChangeResponse(final ResponseType type) {
        reader.read(data, type);
        if (type.equals(ResponseType.RESPONSE_TYPE_CHG)) {
            assertThat(changedFields.toString(), equalTo("{0, 1}"));
        } else {
            assertThat(changedFields.toString(), equalTo("{}"));
        }
    }

    // UPCOMING: invalid field reference

    private List<Object> readFieldValues(final ${type.name}WritableField field, final List<Integer> rows) {
        return rows.stream().map(field::objectValueAt).toList();
    }

    private ${readerType}Data data(final int fieldId, final int... dataSeed) {
        final var builder = ${readerType}Data.newBuilder();
        builder.setFieldId(fieldId);
<#if type.name == "Byte">
        final byte[] data = new byte[dataSeed.length];
        for(int i = 0; i < data.length; i++) data[i] = v(dataSeed[i]);
        builder.setValues(ByteString.copyFrom(data));
<#elseif type.name == "Generic">
        IntStream.of(dataSeed).forEach(seed -> builder.addValues(encoder.encode(v(seed))));
<#else>
        IntStream.of(dataSeed).forEach(seed -> builder.addValues(v(seed)));
</#if>
        return builder.build();
    }

<#if type.name == "Generic">
    private Object v(final int x) {
        return switch(x) {
            case 0 -> Boolean.FALSE;
            case 6 -> (short)x;
            case 7 -> "abc";
            default -> x;
        };
    }
<#else>
    private ${type.arrayType} v(final int x) {
        return ${type.name}Type.castTo${type.name}(x);
    }
</#if>
}
