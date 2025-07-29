<#ftl strip_whitespace=true>
// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.table;

import com.bytefacets.collections.hash.${type.name}IndexedSet;
import com.bytefacets.diaspore.TransformOutput;
import com.bytefacets.diaspore.common.OutputManager;
import com.bytefacets.diaspore.facade.StructFacade;
import com.bytefacets.diaspore.facade.StructFacadeFactory;
import com.bytefacets.diaspore.schema.Schema;
import com.bytefacets.diaspore.schema.SchemaBindable;
import com.bytefacets.diaspore.schema.WritableField;
import com.bytefacets.diaspore.transform.OutputProvider;

import static com.bytefacets.diaspore.exception.DuplicateKeyException.duplicateKeyException;
import static com.bytefacets.diaspore.exception.KeyException.unknownKeyException;
import static java.util.Objects.requireNonNull;

<#if type.name == "Generic">
    <#assign classGenerics="<T, S>">
<#else>
    <#assign classGenerics="<S>">
</#if>
/**
 * A table of your struct, S, keyed by ${type.javaType}. The struct interface S,
 * should adhere to the following guidelines:
 * <ul>
 *   <li>have only getters (get* or *is) and setters (set*)</li>
 *   <li>parameter types of setters and return types of getters should match</li>
 *   <li>setters can, but are not required to, return the interface type to
 *       produce a fluid calling style</li>
 *   <li>do not provide a setter for the key field, only a getter.</li>
 * </ul>
 * The first call to {@link #createFacade()} will use reflection to inspect the type
 * and dynamically create an implementation class of it, which is stored for later use.<p/>
 *
 * Only facades created by this table can be used in the modification methods because
 * the facades created by the table are bound to the underlying storage.<p/>
 */
public final class ${type.name}IndexedStructTable${classGenerics} implements OutputProvider {
    private final OutputManager outputManager;
    private final TableStateChange stateChange;
    private final StructFacadeFactory facadeFactory;
    private final Class<S> structType;
    private final String name;
    private final ${type.name}IndexedSet${generics} index;

    ${type.name}IndexedStructTable(final ${type.name}IndexedSet${generics} index,
                    final Schema schema,
                    final Class<S> structType,
                    final TableStateChange stateChange,
                    final StructFacadeFactory facadeFactory) {
        this.index = requireNonNull(index, "index");
        this.stateChange = requireNonNull(stateChange, "stateChange");
        this.outputManager = OutputManager.outputManager(index::forEachEntry);
        this.outputManager.updateSchema(requireNonNull(schema, "schema"));
        this.facadeFactory = requireNonNull(facadeFactory, "facadeFactory");
        this.structType = requireNonNull(structType, "structType");
        this.name = structType.getSimpleName();
    }

    /** The schema of this table, providing access to the WritableFields that comprise it.*/
    public Schema schema() {
        return outputManager.schema();
    }

    /**
     * Creates an implementation of your interface to use in the table modification
     * methods. The implementation will also implement {@link StructFacade}, which you
     * can use to position the facade over a row yourself.
     */
    public S createFacade() {
        final S facade = facadeFactory.createFacade(structType);
        ((SchemaBindable) facade).bindToSchema(schema().asFieldResolver());
        return facade;
    }

    /**
     * Detaches the facade implementation from the underlying table components.
     * The facade must be one that was returned by this table's {@link createFacade} method.
     */
    public void unbindFacade(final S facade) {
        ((SchemaBindable) facade).unbindSchema();
    }

    /**
     * Begins an add or change operation for the given key and positions the facade
     * over the row. The operation should be closed by endUpsert(). The facade
     * must be one that was returned by this table's {@link createFacade} method.
     */
    public S beginUpsert(final ${type.javaType} key, final S facade) {
        final int before = index.size();
        final int row = index.add(key);
        if(before == index.size()) {
            stateChange.changeRow(row);
        } else {
            stateChange.addRow(row);
        }
        ((StructFacade) facade).moveToRow(row);
        return facade;
    }

    /**
     * Begins an add operation for the given key and positions the facade
     * over the row. The operation should be closed by endAdd(). The facade
     * must be one that was returned by this table's {@link createFacade} method.
     */
    public S beginAdd(final ${type.javaType} key, final S facade) {
        final int before = index.size();
        final int row = index.add(key);
        if(before == index.size()) {
            throw duplicateKeyException(getClass(), name, key);
        }
        stateChange.addRow(row);
        ((StructFacade) facade).moveToRow(row);
        return facade;
    }

    /**
     * Begins an change operation for the given key and positions the facade
     * over the row. The operation should be closed by endChange(). The facade
     * must be one that was returned by this table's {@link createFacade} method.
     */
    public S beginChange(final ${type.javaType} key, final S facade) {
        int row = index.lookupEntry(key);
        if(row == -1) {
            throw unknownKeyException(getClass(), name, key);
        }
        stateChange.changeRow(row);
        ((StructFacade) facade).moveToRow(row);
        return facade;
    }

    /**
     * Convenience method to move to your facade to a given row if you want to
     * read from the row. You can also simplify this if you want to have S
     * extend {@link StructFacade} directly.
     */
    public S moveToRow(final S facade, final int row) {
        ((StructFacade) facade).moveToRow(row);
        return facade;
    }

    /** Returns the row associated with the given key */
    public int lookupKeyRow(final ${type.javaType} key) {
        return index.lookupEntry(key);
    }

    /** Called to close off a {@link beginAdd} and register the row for firing. */
    public void endAdd() {
        stateChange.endAdd();
    }

    /** Called to close off a {@link beginChange} and register the row for firing. */
    public void endChange() {
        stateChange.endChange();
    }

    /** Called to close off a {@link beginUpsert} and register the row for firing. */
    public void endUpsert() {
        stateChange.endUpsert();
    }

    /** Removes the row associated with the key and register the row for firing. */
    public int remove(final ${type.javaType} key) {
        int row = index.lookupEntry(key);
        if(row == -1) {
            throw unknownKeyException(getClass(), name, key);
        }
        index.removeAtAndReserve(row);
        stateChange.removeRow(row);
        return row;
    }

    /** Fires the accumulated changes. */
    public void fireChanges() {
        stateChange.fire(outputManager, index::freeReservedEntry);
    }

    /** The output of this table to which you can attach various inputs to receive updates. */
    public TransformOutput output() {
        return outputManager.output();
    }
}