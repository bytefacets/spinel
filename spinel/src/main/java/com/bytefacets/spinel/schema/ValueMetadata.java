// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.schema;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME) // or CLASS/SOURCE depending on use case
@Target(ElementType.METHOD)
public @interface ValueMetadata {
    /**
     * The content type of the value, e.g. Text, Date, etc
     *
     * @see AttributeConstants.ContentTypes#Text
     * @see AttributeConstants.ContentTypes#Date
     */
    byte contentType() default Byte.MIN_VALUE;

    /**
     * The precision of the value, e.g. Millis or Nanos
     *
     * @see AttributeConstants.Precisions.Timestamp#Micro
     * @see AttributeConstants.Precisions.Timestamp#Milli
     * @see AttributeConstants.Precisions.Timestamp#Nano
     */
    byte precision() default Byte.MIN_VALUE;
}
