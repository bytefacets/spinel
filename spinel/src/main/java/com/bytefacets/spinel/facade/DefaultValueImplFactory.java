// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.facade;

import static com.bytefacets.spinel.gen.DynamicClassFactory.dynamicClassFactory;

import com.bytefacets.spinel.gen.DynamicClassFactory;

/**
 * Implements an interface of only getters and only returns default values for them. Used in places
 * as a stand-in instead of null, in places like GroupBy.
 *
 * @see com.bytefacets.spinel.groupby.RecordAggregationFunction
 */
public final class DefaultValueImplFactory {
    private static final DefaultValueImplFactory instance = new DefaultValueImplFactory();
    private final DynamicClassFactory factory;

    public static DefaultValueImplFactory defaultValueImplFactory() {
        return instance;
    }

    private DefaultValueImplFactory() {
        factory = dynamicClassFactory(new DefaultValueImplBuilder(Inspector.typeInspector()));
    }

    public <T> T createDefaultValue(final Class<T> type) {
        return factory.make(type);
    }
}
