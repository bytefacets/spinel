// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.facade;

import static com.bytefacets.spinel.gen.CodeGenException.invalidUserType;
import static com.bytefacets.spinel.gen.DynamicClassUtils.addToClasspath;
import static com.bytefacets.spinel.gen.DynamicClassUtils.noArgConstructor;
import static com.bytefacets.spinel.gen.DynamicClassUtils.writeMethod;
import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.gen.ClassBuilder;
import com.bytefacets.spinel.schema.TypeId;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Collectors;
import javassist.ClassPool;
import javassist.CtClass;

final class DefaultValueImplBuilder implements ClassBuilder {
    private final Inspector inspector;
    private ClassPool pool;

    DefaultValueImplBuilder(final Inspector inspector) {
        this.inspector = requireNonNull(inspector, "inspector");
    }

    @Override
    public void initClassPool(final ClassPool classPool) {
        this.pool = requireNonNull(classPool, "classPool");
        pool.importPackage("com.bytefacets.spinel.schema");
        pool.importPackage("com.bytefacets.collections.types");
    }

    @Override
    public void buildClass(final Class<?> type, final CtClass dynamicClass) throws Exception {
        final TypeInfo typeInfo = inspector.inspect(type);
        if (typeInfo.fields().isEmpty()) {
            throw invalidUserType(type, "no getters or setters found");
        }
        final List<String> setters =
                typeInfo.fields().stream()
                        .filter(FieldInfo::isWritable)
                        .map(FieldInfo::getName)
                        .toList();
        if (!setters.isEmpty()) {
            throw invalidUserType(type, "setters are invalid for DefaultValueImpl: " + setters);
        }
        if (!typeInfo.skipped().isEmpty()) {
            final String methodNameList =
                    typeInfo.skipped().stream()
                            .map(Method::getName)
                            .collect(Collectors.joining(",", "'", "'"));
            throw invalidUserType(
                    type,
                    "methods found on interface that are not getters or setters -> "
                            + methodNameList);
        }
        dynamicClass.addInterface(pool.get(type.getName()));
        final var state = new DefinitionState(type, dynamicClass, typeInfo);
        state.defineClass();
    }

    @SuppressFBWarnings("VA_FORMAT_STRING_USES_NEWLINE")
    private final class DefinitionState {
        private final Class<?> type;
        private final CtClass dynamicClass;
        private final TypeInfo typeInfo;

        private DefinitionState(
                final Class<?> type, final CtClass dynamicClass, final TypeInfo typeInfo) {
            this.type = type;
            this.dynamicClass = dynamicClass;
            this.typeInfo = typeInfo;
        }

        private String getterMethod(final FieldInfo info, final byte typeId) {
            final String cast =
                    typeId == TypeId.Generic ? "(" + info.type().getSimpleName() + ")" : "";
            return String.format(
                    "public %s %s() { return %s%sType.DEFAULT; }",
                    info.type().getSimpleName(),
                    info.getterMethodName(),
                    cast,
                    TypeId.toTypeName(typeId));
        }

        private void defineClass() {
            noArgConstructor(type, dynamicClass);
            for (FieldInfo fieldInfo : typeInfo.fields()) {
                final byte typeId = TypeId.toId(fieldInfo.type());
                defineGetterIfNecessary(fieldInfo, typeId);
            }
        }

        private void defineGetterIfNecessary(final FieldInfo fieldInfo, final byte typeId) {
            if (fieldInfo.isReadable()) {
                if (typeId == TypeId.Generic) {
                    addToClasspath(pool, fieldInfo.type());
                }
                writeMethod(type, dynamicClass, getterMethod(fieldInfo, typeId));
            }
        }
    }
}
