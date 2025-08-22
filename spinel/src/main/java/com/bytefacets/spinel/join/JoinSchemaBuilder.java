// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.join;

import static com.bytefacets.spinel.exception.FieldNotFoundException.fieldNotFound;
import static com.bytefacets.spinel.schema.FieldMapping.fieldMapping;
import static com.bytefacets.spinel.schema.MappedFieldFactory.asMappedField;
import static com.bytefacets.spinel.schema.SchemaField.schemaField;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.hash.StringGenericIndexedMap;
import com.bytefacets.spinel.common.NameConflictResolver;
import com.bytefacets.spinel.schema.Field;
import com.bytefacets.spinel.schema.FieldList;
import com.bytefacets.spinel.schema.FieldMapping;
import com.bytefacets.spinel.schema.FieldResolver;
import com.bytefacets.spinel.schema.IntField;
import com.bytefacets.spinel.schema.RowMapper;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.SchemaField;
import jakarta.annotation.Nullable;
import java.util.BitSet;

final class JoinSchemaBuilder {
    private final String name;
    private final JoinInterner interner;
    private final String leftSourceRowFieldName;
    private final String rightSourceRowFieldName;
    private final JoinMapper mapper;
    private final NameConflictResolver nameResolver;
    private final JoinKeyHandling joinKeyHandling;

    JoinSchemaBuilder(
            final String name,
            final String leftSourceRowFieldName,
            final String rightSourceRowFieldName,
            final JoinMapper mapper,
            final JoinInterner interner,
            final NameConflictResolver nameResolver,
            final JoinKeyHandling joinKeyHandling) {
        this.name = requireNonNull(name, "name");
        this.mapper = requireNonNull(mapper, "mapper");
        this.interner = requireNonNull(interner, "interner");
        this.leftSourceRowFieldName = leftSourceRowFieldName;
        this.rightSourceRowFieldName = rightSourceRowFieldName;
        this.nameResolver = requireNonNull(nameResolver, "nameResolver");
        this.joinKeyHandling = requireNonNull(joinKeyHandling, "joinKeyHandling");
    }

    void unbindSchemas() {
        interner.unbindSchemas();
    }

    Schema buildSchema(final SchemaResources left, final SchemaResources right) {
        interner.bindToSchemas(left, right);
        final StringGenericIndexedMap<SchemaField> fieldMap =
                new StringGenericIndexedMap<>(left.schema.size() + right.schema.size(), 1f);
        addLeftSourceRowIfNecessary(fieldMap, left.outFieldIds);
        addRightSourceRowIfNecessary(fieldMap, right.outFieldIds);
        addSchema(fieldMap, left, mapper.leftMapper());
        addSchema(fieldMap, right, mapper.rightMapper());
        return Schema.schema(name, FieldList.fieldList(fieldMap));
    }

    private void addSchema(
            final StringGenericIndexedMap<SchemaField> fieldMap,
            final SchemaResources schemaResources,
            final RowMapper rowMapper) {
        final boolean keepJoinKeys = keepJoinKeys(schemaResources.isLeft);
        schemaResources.schema.forEachField(
                inField -> {
                    if (!keepJoinKeys && schemaResources.isJoinKey(inField)) {
                        return;
                    }
                    final String name;
                    if (fieldMap.containsKey(inField.name())) {
                        name =
                                nameResolver.resolveNameConflict(
                                        inField.name(), test -> !fieldMap.containsKey(test));
                        if (name == null) {
                            return;
                        }
                    } else {
                        name = inField.name();
                    }
                    final int outputId = fieldMap.add(name);
                    final var outField = asMappedField(inField.field(), rowMapper);
                    fieldMap.putValueAt(outputId, schemaField(outputId, name, outField));
                    schemaResources.outFieldIds.set(outputId);
                    schemaResources.mappingBuilder.mapInboundToOutbound(
                            inField.fieldId(), outputId);
                });
    }

    private void addLeftSourceRowIfNecessary(
            final StringGenericIndexedMap<SchemaField> fieldMap, final BitSet outIds) {
        if (leftSourceRowFieldName != null) {
            final int fieldId = fieldMap.add(leftSourceRowFieldName);
            final IntField field = mapper.leftMapper()::sourceRowOf;
            fieldMap.putValueAt(fieldId, schemaField(fieldId, leftSourceRowFieldName, field));
            outIds.set(fieldId);
        }
    }

    private void addRightSourceRowIfNecessary(
            final StringGenericIndexedMap<SchemaField> fieldMap, final BitSet outIds) {
        if (rightSourceRowFieldName != null) {
            final int fieldId = fieldMap.add(rightSourceRowFieldName);
            final IntField field = mapper.rightMapper()::sourceRowOf;
            fieldMap.putValueAt(fieldId, schemaField(fieldId, rightSourceRowFieldName, field));
            outIds.set(fieldId);
        }
    }

    static SchemaResources schemaResources(final Schema schema, final boolean isLeft) {
        return new SchemaResources(
                schema, fieldMapping(schema.size()), new BitSet(), new BitSet(), isLeft);
    }

    private boolean keepJoinKeys(final boolean isLeft) {
        if (isLeft) {
            return (joinKeyHandling.equals(JoinKeyHandling.KeepAll)
                    || joinKeyHandling.equals(JoinKeyHandling.KeepLeft));
        } else {
            return (joinKeyHandling.equals(JoinKeyHandling.KeepAll)
                    || joinKeyHandling.equals(JoinKeyHandling.KeepRight));
        }
    }

    record SchemaResources(
            Schema schema,
            FieldMapping.Builder mappingBuilder,
            BitSet joinKeyDependencies,
            BitSet outFieldIds,
            boolean isLeft)
            implements FieldResolver {
        FieldMapping buildFieldMapping() {
            return mappingBuilder.build();
        }

        private boolean isJoinKey(final SchemaField field) {
            return joinKeyDependencies.get(field.fieldId());
        }

        @Nullable
        @Override
        public Field findField(final String name) {
            final SchemaField schemaField = schema.maybeField(name);
            if (schemaField != null) {
                joinKeyDependencies.set(schemaField.fieldId());
                return schemaField.field();
            } else {
                throw fieldNotFound(name, "JoinInterner", schema.name());
            }
        }
    }
}
