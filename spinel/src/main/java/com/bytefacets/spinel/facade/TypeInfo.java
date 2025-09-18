// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.facade;

import static java.util.Objects.requireNonNull;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class TypeInfo {
    private final Class<?> type;
    private final Map<String, FieldInfo> fieldInfoMap = new HashMap<>();
    private final List<Method> skippedMethods = new ArrayList<>(2);

    TypeInfo(final Class<?> type) {
        this.type = requireNonNull(type, "type");
    }

    Class<?> type() {
        return type;
    }

    Collection<FieldInfo> fields() {
        return fieldInfoMap.values();
    }

    Collection<Method> skipped() {
        return skippedMethods;
    }

    void collectGetter(final Method method) {
        final String fieldName = nameFromGetter(method.getName());
        fieldInfoMap
                .computeIfAbsent(fieldName, FieldInfo::new)
                .setReadInfo(method, method.getName(), method.getReturnType());
    }

    void collectSetter(final Method method) {
        final String fieldName = nameFromSetter(method.getName());
        fieldInfoMap
                .computeIfAbsent(fieldName, FieldInfo::new)
                .setWriteInfo(
                        method,
                        method.getName(),
                        method.getParameterTypes()[0],
                        method.getReturnType());
    }

    void collectSkippedMethod(final Method method) {
        skippedMethods.add(method);
    }

    static String nameFromGetter(final String methodName) {
        if (methodName.startsWith("get")) {
            return methodName.substring(3);
        }
        if (methodName.startsWith("is")) {
            return methodName.substring(2);
        }
        return methodName;
    }

    static String nameFromSetter(final String methodName) {
        if (methodName.startsWith("set")) {
            return methodName.substring(3);
        }
        return methodName;
    }
}
