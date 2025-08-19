// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.gen;

import javassist.ClassPool;
import javassist.CtClass;

public interface ClassBuilder {
    void initClassPool(ClassPool classPool);

    void buildClass(Class<?> type, CtClass dynamicClass) throws Exception;
}
