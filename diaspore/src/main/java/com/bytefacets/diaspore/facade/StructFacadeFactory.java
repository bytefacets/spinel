// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.facade;

import static com.bytefacets.diaspore.gen.DynamicClassFactory.dynamicClassFactory;

import com.bytefacets.diaspore.gen.DynamicClassFactory;

public final class StructFacadeFactory {
    private static final StructFacadeFactory instance = new StructFacadeFactory();
    private final DynamicClassFactory factory;

    public static StructFacadeFactory structFacadeFactory() {
        return instance;
    }

    private StructFacadeFactory() {
        factory = dynamicClassFactory(new StructFacadeBuilder(Inspector.typeInspector()));
    }

    public <T> T createFacade(final Class<T> type) {
        return factory.make(type);
    }
}
