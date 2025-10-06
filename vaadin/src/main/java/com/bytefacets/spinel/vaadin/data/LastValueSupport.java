// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.vaadin.data;

import com.bytefacets.spinel.schema.SchemaField;

/**
 * Support interface for managing rendering decisions based on the previous value
 *
 * @see LastValueSupportFactory
 */
public interface LastValueSupport {
    Object evaluate(TransformRow transformRow);

    /**
     * User-specified callback to evaluate the new value, old value, and the comparison result. The
     * result can be used to specify a css class name, for example.
     *
     * @see RendererFactory#changedRenderer(SchemaField, String)
     */
    interface Callback {

        Object callback(Object newValue, Object oldValue, int comparison);
    }
}
