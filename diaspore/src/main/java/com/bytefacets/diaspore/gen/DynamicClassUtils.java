// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.gen;

import static com.bytefacets.diaspore.gen.CodeGenException.codeGenException;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.CtField;
import javassist.CtMethod;

@SuppressWarnings("NeedBraces")
public final class DynamicClassUtils {
    @SuppressFBWarnings("MS_SHOULD_BE_FINAL")
    public static boolean debug = false;

    private DynamicClassUtils() {}

    public static void writeField(
            final Class<?> type, final CtClass newClass, final String fieldDeclaration) {
        if (debug) System.out.println(fieldDeclaration);
        try {
            newClass.addField(CtField.make(fieldDeclaration, newClass));
        } catch (CannotCompileException e) {
            throw codeGenException(type, "writing field", fieldDeclaration, e);
        }
    }

    public static void writeMethod(
            final Class<?> type, final CtClass newClass, final String methodDeclaration) {
        if (debug) System.out.println(methodDeclaration);
        try {
            newClass.addMethod(CtMethod.make(methodDeclaration, newClass));
        } catch (CannotCompileException e) {
            throw codeGenException(type, "writing method", methodDeclaration, e);
        }
    }

    public static void addToClasspath(final ClassPool classPool, final Class<?>... clazz) {
        for (Class<?> c : clazz) {
            classPool.importPackage(c.getName());
        }
    }

    public static void noArgConstructor(final Class<?> type, final CtClass clazz) {
        try {
            final CtConstructor constructor = new CtConstructor(new CtClass[] {}, clazz);
            constructor.setBody("{}");
            clazz.addConstructor(constructor);
        } catch (CannotCompileException e) {
            throw codeGenException(type, "writing no-arg constructor", "{}", e);
        }
    }
}
