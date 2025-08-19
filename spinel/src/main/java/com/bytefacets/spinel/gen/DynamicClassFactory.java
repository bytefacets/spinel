// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.gen;

import static com.bytefacets.spinel.gen.CodeGenException.codeGenException;
import static java.util.Objects.requireNonNull;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import javassist.ClassPool;
import javassist.CtClass;

public final class DynamicClassFactory {
    private static final Object[] NO_ARG = new Object[0];
    private final Map<Class<?>, Constructor<?>> instantiated = new HashMap<>();
    private final ClassPool classPool;
    private final ClassBuilder classBuilder;

    public static DynamicClassFactory dynamicClassFactory(final ClassBuilder classBuilder) {
        return new DynamicClassFactory(classBuilder);
    }

    DynamicClassFactory(final ClassBuilder classBuilder) {
        this.classBuilder = requireNonNull(classBuilder, "classBuilder");
        this.classPool = new ClassPool(ClassPool.getDefault());
        this.classBuilder.initClassPool(this.classPool);
    }

    @SuppressWarnings("unchecked")
    public <T> T make(final Class<T> type) {
        Constructor<?> ctor;
        synchronized (instantiated) {
            ctor = instantiated.get(type);
            if (ctor == null) {
                ctor = process(type);
                instantiated.put(type, ctor);
            }
        }

        try {
            return (T) ctor.newInstance(NO_ARG);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private <T> Constructor<?> process(final Class<T> type) {
        final String className =
                String.format(
                        "%s_%s",
                        type.getName(),
                        Long.toString(System.currentTimeMillis(), 36).toUpperCase());
        final CtClass cc = classPool.makeClass(className);
        try {
            classBuilder.buildClass(type, cc);
            final Class<?> implementationClass = cc.toClass(type);
            return implementationClass.getConstructor();
        } catch (CodeGenException ex) {
            throw ex;
        } catch (Exception e) {
            throw codeGenException(type, "processing class", e);
        }
    }
}
