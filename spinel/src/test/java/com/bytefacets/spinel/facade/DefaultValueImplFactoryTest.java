// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.facade;

import static com.bytefacets.spinel.facade.StructFieldExtractor.consumeFields;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.bytefacets.spinel.gen.CodeGenException;
import com.bytefacets.spinel.schema.AttributeConstants;
import com.bytefacets.spinel.schema.DisplayMetadata;
import com.bytefacets.spinel.schema.FieldDescriptor;
import com.bytefacets.spinel.schema.Metadata;
import com.bytefacets.spinel.schema.ValueMetadata;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DefaultValueImplFactoryTest {
    private final DefaultValueImplFactory factory =
            DefaultValueImplFactory.defaultValueImplFactory();

    @Test
    void shouldThrowWhenReturnTypeDoesNotMatch() {
        final var ex =
                assertThrows(
                        CodeGenException.class,
                        () -> factory.createDefaultValue(InvalidSetter.class));
        assertThat(
                ex.getMessage(),
                containsString("setters are invalid for DefaultValueImpl: [SomeString]"));
    }

    @Test
    void shouldHandleAnnotations() {
        final Map<String, FieldDescriptor> map = new HashMap<>();
        consumeFields(WithAnnotations.class, f -> {}, f -> map.put(f.name(), f));
        // formatting:off
        assertThat(
            map.get("SomeLong").metadata(),
            equalTo(Metadata.metadata(Map.of(
                AttributeConstants.ContentType, AttributeConstants.ContentTypes.Timestamp,
                AttributeConstants.ValuePrecision, AttributeConstants.Precisions.Timestamp.Milli,
                AttributeConstants.DisplayPrecision, AttributeConstants.Precisions.Timestamp.Micro,
                AttributeConstants.DisplayFormat, "#,000.00",
                AttributeConstants.TimeZone, "UTC"))));
        assertThat(map.get("SomeInt").metadata(), equalTo(Metadata.EMPTY));
        // formatting:on
    }

    @Test
    void shouldHandleAnnotationsThruInheritance() {
        final Map<String, FieldDescriptor> map = new HashMap<>();
        consumeFields(InheritSetter.class, f -> map.put(f.name(), f), f -> map.put(f.name(), f));
        // formatting:off
        assertThat(
                map.get("SomeLong").metadata(),
                equalTo(Metadata.metadata(Map.of(
                        AttributeConstants.ContentType, AttributeConstants.ContentTypes.Timestamp,
                        AttributeConstants.ValuePrecision, AttributeConstants.Precisions.Timestamp.Milli,
                        AttributeConstants.DisplayPrecision, AttributeConstants.Precisions.Timestamp.Micro,
                        AttributeConstants.DisplayFormat, "#,000.00",
                        AttributeConstants.TimeZone, "UTC"))));
        assertThat(map.get("SomeInt").metadata(), equalTo(Metadata.EMPTY));
        assertThat(
                map.get("SomeDouble").metadata(),
                equalTo(Metadata.metadata(Map.of(
                        AttributeConstants.ContentType, AttributeConstants.ContentTypes.Percent,
                        AttributeConstants.DisplayFormat, "#,000.00%"))));
        // formatting:on
    }

    @Test
    void shouldThrowWhenOtherThanGettersAndSetters() {
        final var ex =
                assertThrows(
                        CodeGenException.class,
                        () -> factory.createDefaultValue(InvalidNonGetter.class));
        assertThat(
                ex.getMessage(),
                containsString(
                        "methods found on interface that are not getters or setters -> 'random'"));
    }

    @Test
    void shouldThrowWhenNoUserFields() {
        final var ex =
                assertThrows(
                        CodeGenException.class,
                        () -> factory.createDefaultValue(InvalidNoUserFields.class));
        assertThat(ex.getMessage(), containsString("no getters or setters found"));
    }

    @Test
    void shouldReturnAllTypesOfDefaults() {
        final AllTypes noValue = factory.createDefaultValue(AllTypes.class);
        assertThat(noValue.getSomeBool(), equalTo(false));
        assertThat(noValue.getSomeByte(), equalTo((byte) 0));
        assertThat(noValue.getSomeShort(), equalTo((short) 0));
        assertThat(noValue.getSomeChar(), equalTo('\0'));
        assertThat(noValue.getSomeInt(), equalTo(0));
        assertThat(noValue.getSomeLong(), equalTo(0L));
        assertThat(noValue.getSomeFloat(), equalTo(0f));
        assertThat(noValue.getSomeDouble(), equalTo(0d));
        assertThat(noValue.getSomeString(), equalTo(null));
        assertThat(noValue.getSomeObject(), equalTo(null));
        assertThat(noValue.getSomeGeneric(), equalTo(null));
    }

    // formatting:off
    public interface AllTypes {
        boolean getSomeBool();
        byte getSomeByte();
        short getSomeShort();
        char getSomeChar();
        int getSomeInt();
        long getSomeLong();
        float getSomeFloat();
        double getSomeDouble();
        String getSomeString();
        Object getSomeObject();
        LocalDate getSomeGeneric();
    }

    public interface InvalidNoUserFields {
        void setSomeString(); // not a valid setter: no arg
        void random(); // not a valid getter: no return type or get/is
    }

    public interface InvalidNonGetter {
        String getSomeString();
        void random();
    }

    public interface InvalidSetter {
        void setSomeString(String value);
    }
    // formatting:on

    public interface WithAnnotations {
        @ValueMetadata(
                contentType = AttributeConstants.ContentTypes.Timestamp,
                precision = AttributeConstants.Precisions.Timestamp.Milli)
        @DisplayMetadata(
                format = "#,000.00",
                zoneId = "UTC",
                precision = AttributeConstants.Precisions.Timestamp.Micro)
        long getSomeLong();

        @ValueMetadata()
        @DisplayMetadata()
        int getSomeInt();
    }

    public interface InheritGetter {
        @ValueMetadata(
                contentType = AttributeConstants.ContentTypes.Timestamp,
                precision = AttributeConstants.Precisions.Timestamp.Milli)
        @DisplayMetadata(
                format = "#,000.00",
                zoneId = "UTC",
                precision = AttributeConstants.Precisions.Timestamp.Micro)
        long getSomeLong();

        @ValueMetadata()
        @DisplayMetadata()
        int getSomeInt();
    }

    public interface InheritSetter extends InheritGetter {
        @ValueMetadata()
        @DisplayMetadata()
        void setSomeLong(long value);

        @ValueMetadata()
        @DisplayMetadata()
        void setSomeInt(int value);

        @ValueMetadata(contentType = AttributeConstants.ContentTypes.Percent)
        @DisplayMetadata(format = "#,000.00%")
        void setSomeDouble(double value);
    }
}
