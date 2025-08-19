<#ftl strip_whitespace=true>
package com.bytefacets.spinel.interner;
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT

import com.bytefacets.spinel.schema.TypeId;
<#list types as type>
<#if type.name != "Bool">
import com.bytefacets.collections.hash.${type.name}IndexedSet;
</#if>
</#list>

@SuppressWarnings("unchecked")
public final class InternSetProvider {
    private Object set;
    private byte typeId = -1;
    private boolean isTuple = false;

    public void reset() {
        set = null;
        typeId = -1;
        isTuple = false;
    }

<#list types as type>
<#if type.name != "Bool">
    public ${type.name}IndexedSet${type.instanceGenerics} getOrCreate${type.name}Set(final int initialCapacity) {
        if(set == null) {
            final var typedSet = new ${type.name}IndexedSet${type.instanceGenerics}(initialCapacity);
            this.set = typedSet;
            typeId = TypeId.${type.name};
            isTuple = false;
            return typedSet;
        } else {
            validateOrThrow(TypeId.${type.name}, false);
            return (${type.name}IndexedSet${type.instanceGenerics})set;
        }
    }
</#if>
</#list>

    public GenericIndexedSet<OpaqueTuple> getOrCreateTupleSet(final int initialCapacity) {
        if(set == null) {
            final var typedSet = new GenericIndexedSet<OpaqueTuple>(initialCapacity);
            this.set = typedSet;
            typeId = TypeId.Generic;
            isTuple = true;
            return typedSet;
        } else {
            validateOrThrow(TypeId.Generic, true);
            return (GenericIndexedSet<OpaqueTuple>)set;
        }
    }

    private void validateOrThrow(final byte requestedTypeId, final boolean requestedTuple) {
        if(requestedTypeId != typeId || requestedTuple != isTuple) {
            throw new IllegalStateException(String.format(
                    "Requested set type (%d,%b) does not match the set already produced: (%d%b)",
                    requestedTypeId, requestedTuple, typeId, isTuple));
        }
    }
}
