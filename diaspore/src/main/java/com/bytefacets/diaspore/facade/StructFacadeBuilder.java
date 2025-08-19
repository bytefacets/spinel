// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.facade;

import static com.bytefacets.diaspore.gen.CodeGenException.codeGenException;
import static com.bytefacets.diaspore.gen.CodeGenException.invalidUserType;
import static com.bytefacets.diaspore.gen.DynamicClassUtils.addToClasspath;
import static com.bytefacets.diaspore.gen.DynamicClassUtils.noArgConstructor;
import static com.bytefacets.diaspore.gen.DynamicClassUtils.writeField;
import static com.bytefacets.diaspore.gen.DynamicClassUtils.writeMethod;
import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.gen.ClassBuilder;
import com.bytefacets.diaspore.schema.FieldResolver;
import com.bytefacets.diaspore.schema.SchemaBindable;
import com.bytefacets.diaspore.schema.TypeId;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.lang.reflect.Method;
import java.util.stream.Collectors;
import javassist.ClassPool;
import javassist.CtClass;

final class StructFacadeBuilder implements ClassBuilder {
    private final Inspector inspector;
    private ClassPool pool;

    StructFacadeBuilder(final Inspector inspector) {
        this.inspector = requireNonNull(inspector, "inspector");
    }

    @Override
    public void initClassPool(final ClassPool classPool) {
        this.pool = requireNonNull(classPool, "classPool");
        pool.importPackage("com.bytefacets.diaspore.schema");
        addToClasspath(pool, FieldResolver.class, StructFacade.class);
    }

    @Override
    public void buildClass(final Class<?> type, final CtClass dynamicClass) throws Exception {
        final TypeInfo typeInfo = inspector.inspect(type);
        if (typeInfo.fields().isEmpty()) {
            throw invalidUserType(type, "no getters or setters found");
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
        dynamicClass.addInterface(pool.get(StructFacade.class.getName()));
        dynamicClass.addInterface(pool.get(SchemaBindable.class.getName()));
        final var state = new DefinitionState(type, dynamicClass, typeInfo);
        state.defineClass();
    }

    @SuppressFBWarnings("VA_FORMAT_STRING_USES_NEWLINE")
    private final class DefinitionState {
        private final Class<?> type;
        private final CtClass dynamicClass;
        private final TypeInfo typeInfo;
        private final StringBuilder bind = new StringBuilder(128);
        private final StringBuilder unbind = new StringBuilder(128);

        private DefinitionState(
                final Class<?> type, final CtClass dynamicClass, final TypeInfo typeInfo) {
            this.type = type;
            this.dynamicClass = dynamicClass;
            this.typeInfo = typeInfo;
        }

        private void writeRowHandlers() {
            writeField(type, dynamicClass, "private int row = -1;");
            writeMethod(type, dynamicClass, "public int currentRow() { return row; }");
            writeMethod(type, dynamicClass, "public void moveToRow(int row) { this.row = row; }");
        }

        private String getterMethod(final FieldInfo info, final byte typeId) {
            final String cast =
                    typeId == TypeId.Generic ? "(" + info.type().getSimpleName() + ")" : "";
            return String.format(
                    "public %s %s() { return %s_f%s.valueAt(row); }",
                    info.type().getSimpleName(), info.getterMethodName(), cast, info.getName());
        }

        private String setterMethod(final FieldInfo info) {
            final String returnType = info.setterReturnType().getTypeName();
            final String returnStatement =
                    returnType.equals("void") ? "" : validateFluidReturn(info);
            return String.format(
                    "public %s %s(%s value) { _f%s.setValueAt(row, value); %s }",
                    returnType,
                    info.setterMethodName(),
                    info.type().getName(),
                    info.getName(),
                    returnStatement);
        }

        private String validateFluidReturn(final FieldInfo info) {
            final var returnType = info.setterReturnType();
            if (type.isAssignableFrom(returnType)) {
                return "return this;";
            } else if (!returnType.equals(Void.class)) {
                final String msg =
                        String.format(
                                "Cannot cast %s to %s", type.getSimpleName(), returnType.getName());
                throw codeGenException(
                        type,
                        "implementing return of " + info.setterMethodName(),
                        new ClassCastException(msg));
            }
            return "";
        }

        private String fieldDeclaration(final FieldInfo info, final String typeName) {
            final String fieldType = typeName + (info.isWritable() ? "Writable" : "") + "Field";
            return String.format("private %s _f%s;", fieldType, info.getName());
        }

        private void defineClass() {
            noArgConstructor(type, dynamicClass);
            writeRowHandlers();
            bind.append("public void bindToSchema(FieldResolver fieldResolver) {\n");
            unbind.append("public void unbindSchema() {\n");
            for (FieldInfo fieldInfo : typeInfo.fields()) {
                final String name = fieldInfo.getName();
                final byte typeId = TypeId.toId(fieldInfo.type());
                final String typeName = TypeId.toTypeName(typeId);
                writeField(type, dynamicClass, fieldDeclaration(fieldInfo, typeName));
                defineGetterIfNecessary(fieldInfo, typeId);
                defineSetterIfNecessary(fieldInfo, typeId);
                bind.append(bindStatement(name, typeName));
                unbind.append(unbindStatement(name));
            }
            bind.append("}\n");
            unbind.append("}\n");
            writeMethod(type, dynamicClass, bind.toString());
            writeMethod(type, dynamicClass, unbind.toString());
        }

        private String unbindStatement(final String name) {
            return String.format("_f%s = null;\n", name);
        }

        private String bindStatement(final String name, final String typeName) {
            return String.format(
                    "_f%s = fieldResolver.find%sField(\"%s\");%n", name, typeName, name);
        }

        private void defineSetterIfNecessary(final FieldInfo fieldInfo, final byte typeId) {
            if (fieldInfo.isWritable()) {
                if (typeId == TypeId.Generic) {
                    addToClasspath(pool, fieldInfo.type());
                }
                writeMethod(type, dynamicClass, setterMethod(fieldInfo));
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
