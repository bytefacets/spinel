// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.facade;

import static com.bytefacets.spinel.gen.CodeGenException.codeGenException;

import com.bytefacets.spinel.schema.SchemaBindable;
import java.lang.reflect.Method;
import java.util.stream.Collectors;

final class Inspector {
    private static final Inspector INSTANCE = new Inspector();

    private Inspector() {}

    static Inspector typeInspector() {
        return INSTANCE;
    }

    TypeInfo inspect(final Class<?> type) {
        final TypeInfo typeInfo = new TypeInfo(type);
        collectMethodInfo(typeInfo);
        throwIfTypeInconsistency(typeInfo);
        return typeInfo;
    }

    private void throwIfTypeInconsistency(final TypeInfo typeInfo) {
        final var inconsistent =
                typeInfo.fields().stream().filter(fInfo -> !fInfo.isTypeConsistent()).toList();
        if (!inconsistent.isEmpty()) {
            final String message =
                    inconsistent.stream()
                            .map(
                                    f ->
                                            String.format(
                                                    "%s type inconsistent between get type(%s) and set type(%s)",
                                                    f.getName(), f.getType(), f.setType()))
                            .collect(Collectors.joining(";"));
            throw codeGenException(
                    typeInfo.type(), "inspecting methods", new ClassCastException(message));
        }
    }

    private void collectMethodInfo(final TypeInfo typeInfo) {
        for (Method method : typeInfo.type().getMethods()) {
            eval(typeInfo, method);
        }
    }

    private void eval(final TypeInfo typeInfo, final Method method) {
        if (isGetterMethod(method)) {
            typeInfo.collectGetter(method);
        } else if (isSetterMethod(method)) {
            typeInfo.collectSetter(method);
        } else if (!isSpinelIfc(method)) {
            typeInfo.collectSkippedMethod(method);
        }
    }

    private boolean isSpinelIfc(final Method method) {
        return method.getDeclaringClass().equals(StructFacade.class)
                || method.getDeclaringClass().equals(SchemaBindable.class);
    }

    static boolean isGetterMethod(final Method method) {
        if (method.getParameterCount() != 0) {
            return false;
        }
        return method.getName().startsWith("get") || method.getName().startsWith("is");
    }

    static boolean isSetterMethod(final Method method) {
        if (method.getParameterCount() != 1) {
            return false;
        }
        return method.getName().startsWith("set");
    }
}
