<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.interner;

import com.bytefacets.collections.hash.GenericIndexedSet;
import com.bytefacets.diaspore.schema.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public final class TupleInterner implements RowInterner {
    private static final byte[] WIDTH = new byte[TypeId.Max+1];
    static {
        WIDTH[TypeId.Bool] = 1;
        WIDTH[TypeId.Byte] = 1;
        WIDTH[TypeId.Short] = 2;
        WIDTH[TypeId.Char] = 2;
        WIDTH[TypeId.Int] = 4;
        WIDTH[TypeId.Long] = 8;
        WIDTH[TypeId.Float] = 4;
        WIDTH[TypeId.Double] = 8;
        WIDTH[TypeId.String] = 1;
        WIDTH[TypeId.Generic] = 1;
    }
    private final SharedState state = new SharedState();
    private final List<FieldCopier> copiers;
    private final GenericIndexedSet<OpaqueTuple> set;
    private final List<String> fields;
    private Function<SharedState, OpaqueTuple> tupleMethod;

    public TupleInterner(final List<String> fields, final int initialCapacity) {
        this(fields, new GenericIndexedSet<>(initialCapacity));
    }

    public TupleInterner(final List<String> fields, final GenericIndexedSet<OpaqueTuple> set) {
        this.fields = Objects.requireNonNull(fields, "fields");
        this.set = Objects.requireNonNull(set, "set");
        this.copiers = new ArrayList<>(fields.size());
    }

    @Override
    public int intern(final int row) {
        copiers.forEach(copier -> copier.copy(row));
        final var tuple = tupleMethod.apply(state);
        state.reset();
        return set.add(tuple);
    }

    @Override
    public void freeEntry(final int entry) {
        set.removeAt(entry);
    }

    @Override
    public void bindToSchema(final FieldResolver fieldResolver) {
        int fixedWidth = 0;
        int objectCount = 0;
        for(String name : fields) {
            final var field = fieldResolver.getField(name);
            copiers.add(createCopier(field));
            if(field.typeId() == TypeId.String || field.typeId() == TypeId.Generic) {
                objectCount++;
            } else {
                fixedWidth += WIDTH[field.typeId()];
            }
        }
        state.initialize(fixedWidth, objectCount);
        if(objectCount == 0) {
            tupleMethod = SharedState::createFixedLength;
        } else if(fixedWidth == 0) {
            tupleMethod = SharedState::createObject;
        } else {
            tupleMethod = SharedState::createMixed;
        }
    }

    @Override
    public void unbindSchema() {
        set.clear();
        copiers.clear();
    }

    private FieldCopier createCopier(final Field field) {
        return switch(field.typeId()) {
<#list types as type>
            case TypeId.${type.name} -> new ${type.name}Copier((${type.name}Field)field, state);
</#list>
            default -> throw new IllegalArgumentException("Unhandled type: " + field.typeId());
        };
    }

    private static class SharedState {
        private Object[] objects;
        private ByteBuffer buffer;
        private int objectPos = 0;
        private void initialize(final int bufferLength, final int objectCount) {
            buffer = ByteBuffer.wrap(new byte[bufferLength]);
            objects = new Object[objectCount];
        }

        private void addObject(final Object o) {
            objects[objectPos++] = o;
        }

        private byte[] copyArray() {
            return Arrays.copyOf(buffer.array(), buffer.capacity());
        }

        private Object[] copyObjects() {
            return Arrays.copyOf(objects, objects.length);
        }

        private FixedLengthTuple createFixedLength() {
            return new FixedLengthTuple(copyArray());
        }

        private MixedTuple createMixed() {
            return new MixedTuple(copyArray(), copyObjects());
        }

        private ObjectTuple createObject() {
            return new ObjectTuple(copyObjects());
        }

        void reset() {
            buffer.clear();
            objectPos = 0;
            Arrays.fill(objects, null);
        }
    }

<#list types as type>
<#if type.name == "String" || type.name == "Generic">
    private static class ${type.name}Copier implements FieldCopier {
        private final ${type.name}Field field;
        private final SharedState state;
        private ${type.name}Copier(final ${type.name}Field field, final SharedState state) {
            this.field = Objects.requireNonNull(field, "field");
            this.state = Objects.requireNonNull(state, "state");
        }

        @Override
        public void copy(final int row) {
             state.addObject(field.valueAt(row));
        }
    }
<#else>
    private static class ${type.name}Copier implements FieldCopier {
        private final ${type.name}Field field;
        private final SharedState state;
        private ${type.name}Copier(final ${type.name}Field field, final SharedState state) {
            this.field = Objects.requireNonNull(field, "field");
            this.state = Objects.requireNonNull(state, "state");
        }

        @Override
        public void copy(final int row) {
             final var value = field.valueAt(row);
    <#if type.name == "Bool">
             state.buffer.put((byte)(value?1:0));
    <#elseif type.name == "Byte">
             state.buffer.put(value);
    <#else>
             state.buffer.put${type.name}(value);
    </#if>
        }
    }
</#if>

</#list>
}