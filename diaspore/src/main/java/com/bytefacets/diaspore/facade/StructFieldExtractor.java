package com.bytefacets.diaspore.facade;

import com.bytefacets.diaspore.schema.FieldDescriptor;
import com.bytefacets.diaspore.schema.Metadata;
import com.bytefacets.diaspore.schema.TypeId;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public final class StructFieldExtractor {

    public static List<FieldDescriptor> getFieldList(final Class<?> type) {
        final TypeInfo info = Inspector.typeInspector().inspect(type);
        final var fields = info.fields();
        final List<FieldDescriptor> descriptors = new ArrayList<>(fields.size());
        fields.forEach(
                field -> {
                    if (field.isWritable()) {
                        descriptors.add(
                                new FieldDescriptor(
                                        TypeId.toId(field.type()),
                                        field.getName(),
                                        Metadata.EMPTY));
                    }
                });
        return descriptors;
    }

    public static void consumeFields(
            final Class<?> type,
            final Consumer<FieldDescriptor> writableFieldConsumer,
            final Consumer<FieldDescriptor> readableFieldConsumer) {
        final TypeInfo info = Inspector.typeInspector().inspect(type);
        final var fields = info.fields();
        fields.forEach(
                field -> {
                    final var fd =
                            new FieldDescriptor(
                                    TypeId.toId(field.type()), field.getName(), Metadata.EMPTY);
                    if (field.isWritable()) {
                        writableFieldConsumer.accept(fd);
                    } else {
                        readableFieldConsumer.accept(fd);
                    }
                });
    }
}
