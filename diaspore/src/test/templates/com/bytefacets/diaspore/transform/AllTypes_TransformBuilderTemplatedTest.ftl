package com.bytefacets.diaspore.transform;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.bytefacets.collections.types.*;
import com.bytefacets.diaspore.TransformInput;
import com.bytefacets.diaspore.schema.FieldDescriptor;
import com.bytefacets.diaspore.schema.IntWritableField;
import com.bytefacets.diaspore.filter.lib.IntPredicate;
import com.bytefacets.diaspore.table.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.function.Consumer;
import java.util.stream.IntStream;

@ExtendWith(MockitoExtension.class)
class TransformBuilderTemplatedTest {
    private final TransformBuilder transform = TransformBuilder.transform();
    private @Mock TransformInput input;

    @BeforeEach
    void setUp() {
    }

<#list types as type>
<#if type.name != "Bool">
    @Test
    void shouldConnect${type.name}IndexedTable() {
        addFilter(transform.${type.name?lower_case}IndexedTable("test")
            .addField(FieldDescriptor.intField("F1"))
            .addField(FieldDescriptor.intField("F2"))
            .then());
        final Consumer<${type.name}IndexedTable${type.instanceGenerics}> consumer = table -> {
            IntStream.range(0, 10).forEach(i -> {
                final int row = table.beginAdd(${type.name}Type.castTo${type.name}(i+1));
                ((IntWritableField) table.writableField("F1")).setValueAt(row, i+11);
                table.endAdd();
            });
            table.fireChanges();
        };
        validate(consumer, "test");
    }

    <#if type.name = "Generic">
        <#assign instanceGenerics="<Object, ${type.name}Struct>">
    <#else>
        <#assign instanceGenerics="<${type.name}Struct>">
    </#if>
    @Test
    void shouldConnect${type.name}IndexedStructTable() {
        addFilter(transform.${type.name?lower_case}IndexedStructTable(${type.name}Struct.class).then());
        final Consumer<${type.name}IndexedStructTable${instanceGenerics}> consumer = table -> {
            final var facade = table.createFacade();
            IntStream.range(0, 10).forEach(i -> {
                table.beginAdd(${type.name}Type.castTo${type.name}(i+1), facade).setF1(i+11);
                table.endAdd();
            });
            table.fireChanges();
        };
        validate(consumer, "${type.name}Struct");
    }

    interface ${type.name}Struct {
        ${type.arrayType} getKey();
        void setF1(int value);
        void setF2(int value);
    }
</#if>
</#list>

    private void addFilter(final TransformContinuation continuation) {
        continuation.filter("x").initialPredicate(IntPredicate.intPredicate("F1", i -> i % 2 == 0))
            .build().output().attachInput(input);
    }

    private <T> void validate(final Consumer<T> adder, final String name) {
        transform.build();
        adder.accept(transform.lookupNode(name));
        verify(input, times(1)).setSource(any());
        verify(input, times(1)).schemaUpdated(any());
        verify(input, times(1)).rowsAdded(any());
    }
}
