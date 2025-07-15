package com.bytefacets.diaspore.grpc.receive;

import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.hash.StringGenericIndexedMap;
import com.bytefacets.diaspore.grpc.proto.FieldDefinition;
import com.bytefacets.diaspore.grpc.proto.SchemaUpdate;
import com.bytefacets.diaspore.schema.FieldDescriptor;
import com.bytefacets.diaspore.schema.FieldList;
import com.bytefacets.diaspore.schema.MatrixStoreFieldFactory;
import com.bytefacets.diaspore.schema.Metadata;
import com.bytefacets.diaspore.schema.Schema;
import com.bytefacets.diaspore.schema.SchemaField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

final class SchemaBuilder {
    private final MatrixStoreFieldFactory fieldFactory;

    SchemaBuilder(final MatrixStoreFieldFactory fieldFactory) {
        this.fieldFactory = requireNonNull(fieldFactory, "fieldFactory");
    }

    Schema createSchema(final SchemaUpdate schemaUpdate) {
        final int fieldCt = schemaUpdate.getFieldsCount();
        final var fieldMap = new StringGenericIndexedMap<SchemaField>(fieldCt, 1f);
        final Map<Byte, List<FieldDescriptor>> typeMap = new HashMap<>();
        for (int i = 0; i < fieldCt; i++) {
            final var def = schemaUpdate.getFields(i);
            final byte typeId = (byte) def.getTypeId();
            final Metadata metadata = readMetadata(def);
            final var fieldDesc = new FieldDescriptor(typeId, def.getName(), metadata);
            typeMap.computeIfAbsent(typeId, k -> new ArrayList<>(4)).add(fieldDesc);
            fieldMap.add(def.getName());
        }
        return createSchema(schemaUpdate.getName(), fieldMap, typeMap);
    }

    private Schema createSchema(
            final String schemaName,
            final StringGenericIndexedMap<SchemaField> fieldMap,
            final Map<Byte, List<FieldDescriptor>> typeMap) {
        final FieldList fields = fieldFactory.createFieldList(fieldMap, typeMap);
        return Schema.schema(schemaName, fields);
    }

    private @Nullable Metadata readMetadata(final FieldDefinition def) {
        final var protoMetadata = def.getMetadata();
        final int attrCount = protoMetadata.getAttributesCount();
        final int tagCount = protoMetadata.getTagsCount();
        if (attrCount == 0 && tagCount == 0) {
            return null;
        }
        final Set<String> tags = tagCount > 0 ? readTags(protoMetadata) : null;
        final Map<String, Object> attrs = attrCount > 0 ? readAttrs(protoMetadata) : null;
        return Metadata.metadata(tags, attrs);
    }

    private Map<String, Object> readAttrs(
            final com.bytefacets.diaspore.grpc.proto.Metadata protoMetadata) {
        final int attrCount = protoMetadata.getAttributesCount();
        final Map<String, Object> attrs = new HashMap<>(attrCount, 1f);
        protoMetadata
                .getAttributesMap()
                .forEach((name, bytes) -> attrs.put(name, ObjectDecoderRegistry.decode(bytes)));
        return attrs;
    }

    private Set<String> readTags(final com.bytefacets.diaspore.grpc.proto.Metadata protoMetadata) {
        final int tagCount = protoMetadata.getTagsCount();
        final Set<String> tags = new HashSet<>(tagCount, 1f);
        for (int i = 0; i < tagCount; i++) {
            tags.add(protoMetadata.getTags(i));
        }
        return tags;
    }
}
