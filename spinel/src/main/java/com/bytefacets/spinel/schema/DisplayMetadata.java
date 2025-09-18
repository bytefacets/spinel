// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.schema;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME) // or CLASS/SOURCE depending on use case
@Target(ElementType.METHOD)
public @interface DisplayMetadata {
    /**
     * A format string or predefined constants for numeric values.
     *
     * @see AttributeConstants.DisplayFormats#BigEndian
     * @see AttributeConstants.DisplayFormats#LittleEndian
     * @see AttributeConstants.DisplayFormats#kMBT
     */
    String format() default "";

    /** A zoneId to use when rendering a time-related value */
    String zoneId() default "";

    /**
     * The precision to render in a display, e.g. Millis or Nanos
     *
     * @see AttributeConstants.Precisions.Timestamp#Micro
     * @see AttributeConstants.Precisions.Timestamp#Milli
     * @see AttributeConstants.Precisions.Timestamp#Nano
     */
    byte precision() default Byte.MIN_VALUE;
}
