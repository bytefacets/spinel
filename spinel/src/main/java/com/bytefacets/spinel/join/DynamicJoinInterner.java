// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.join;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.hash.GenericIndexedSet;
import com.bytefacets.spinel.interner.ConstantRowInterner;
import com.bytefacets.spinel.interner.InternSetProvider;
import com.bytefacets.spinel.interner.InternerFactory;
import com.bytefacets.spinel.interner.OpaqueTuple;
import com.bytefacets.spinel.interner.RowInterner;
import com.bytefacets.spinel.interner.TupleInterner;
import com.bytefacets.spinel.schema.FieldResolver;
import java.util.Arrays;
import java.util.List;

public final class DynamicJoinInterner implements JoinInterner {
    private final List<String> leftNames;
    private final List<String> rightNames;
    private final int initialCapacity;
    private final InternSetProvider setProvider;
    private RowInterner left;
    private RowInterner right;

    public static DynamicJoinInterner dynamicJoinInterner(
            final List<String> leftNames,
            final List<String> rightNames,
            final int initialCapacity) {
        return new DynamicJoinInterner(leftNames, rightNames, initialCapacity);
    }

    DynamicJoinInterner(
            final List<String> leftNames,
            final List<String> rightNames,
            final int initialCapacity) {
        this.leftNames = requireNonNull(leftNames, "leftNames");
        this.rightNames = requireNonNull(rightNames, "rightNames");
        this.initialCapacity = initialCapacity;
        this.setProvider = InternSetProvider.internSetProvider();
    }

    @Override
    public void bindToSchemas(final FieldResolver leftResolver, final FieldResolver rightResolver) {
        final byte[] types = validateTypes(leftResolver, rightResolver);
        if (types.length == 0) {
            left = ConstantRowInterner.Instance;
            right = ConstantRowInterner.Instance;
        } else if (types.length == 1) {
            left = singleFieldInterner(leftNames.get(0), types[0]);
            right = singleFieldInterner(rightNames.get(0), types[0]);
        } else {
            final GenericIndexedSet<OpaqueTuple> set = new GenericIndexedSet<>(initialCapacity);
            left = new TupleInterner(leftNames, set);
            right = new TupleInterner(rightNames, set);
        }
        left.bindToSchema(leftResolver);
        right.bindToSchema(rightResolver);
    }

    @Override
    public void unbindSchemas() {
        if (left != null) {
            left.unbindSchema();
            left = null;
        }
        if (right != null) {
            right.unbindSchema();
            right = null;
        }
        setProvider.reset();
    }

    private byte[] validateTypes(
            final FieldResolver leftResolver, final FieldResolver rightResolver) {
        final byte[] leftTypes = readTypes(leftNames, leftResolver);
        final byte[] rightTypes = readTypes(rightNames, rightResolver);
        if (!Arrays.equals(leftTypes, rightTypes)) {
            throw new RuntimeException(
                    String.format(
                            "Join types don't match: left=%s, right=%s",
                            Arrays.toString(leftTypes), Arrays.toString(rightTypes)));
        }
        return leftTypes;
    }

    private RowInterner singleFieldInterner(final String fieldName, final byte typeId) {
        return InternerFactory.interner(fieldName, typeId, initialCapacity, setProvider);
    }

    private byte[] readTypes(final List<String> names, final FieldResolver resolver) {
        final byte[] types = new byte[names.size()];
        for (int i = 0; i < types.length; i++) {
            types[i] = resolver.getField(names.get(i)).typeId();
        }
        return types;
    }

    @Override
    public RowInterner left() {
        return left;
    }

    @Override
    public RowInterner right() {
        return right;
    }
}
