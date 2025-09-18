// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.facade;

import static java.util.Objects.requireNonNull;

import java.lang.reflect.Method;

final class FieldInfo {
    private final String name;
    private String getterMethodName;
    private String setterMethodName;
    private Class<?> getterType;
    private Class<?> setterType;
    private Class<?> setterReturnType;
    private Method getterMethod;
    private Method setterMethod;

    FieldInfo(final String name) {
        this.name = requireNonNull(name, "name");
    }

    String getName() {
        return name;
    }

    Class<?> type() {
        return getterType != null ? getterType : setterType;
    }

    Class<?> getType() {
        return getterType;
    }

    Class<?> setType() {
        return setterType;
    }

    boolean isTypeConsistent() {
        return getterType == null || setterType == null || setterType.equals(getterType);
    }

    boolean isWritable() {
        return setterMethodName != null;
    }

    boolean isReadable() {
        return getterMethodName != null;
    }

    FieldInfo setReadInfo(final Method method, final String methodName, final Class<?> returnType) {
        this.getterMethod = requireNonNull(method, "method");
        this.getterMethodName = requireNonNull(methodName, "getterMethodName");
        this.getterType = requireNonNull(returnType, "returnType");
        return this;
    }

    FieldInfo setWriteInfo(
            final Method method,
            final String methodName,
            final Class<?> paramType,
            final Class<?> returnType) {
        this.setterMethod = requireNonNull(method, "method");
        this.setterMethodName = requireNonNull(methodName, "setterMethodName");
        this.setterType = requireNonNull(paramType, "paramType");
        this.setterReturnType = requireNonNull(returnType, "returnType");
        return this;
    }

    Method setterMethod() {
        return setterMethod;
    }

    Method getterMethod() {
        return getterMethod;
    }

    String getterMethodName() {
        return getterMethodName;
    }

    String setterMethodName() {
        return setterMethodName;
    }

    Class<?> setterReturnType() {
        return setterReturnType;
    }
}
