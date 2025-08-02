package com.bytefacets.diaspore.transform;

import static com.bytefacets.diaspore.filter.FilterBuilder.filter;
import static com.bytefacets.diaspore.schema.FieldDescriptor.intField;
import static com.bytefacets.diaspore.table.IntIndexedTableBuilder.intIndexedTable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.bytefacets.diaspore.TransformInput;
import com.bytefacets.diaspore.TransformOutput;
import com.bytefacets.diaspore.schema.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TransformBuilderTest {
    private final TransformBuilder builder = TransformBuilder.transform();
    private @Mock OutputProvider outputProvider;
    private @Mock TransformOutput output;
    private @Mock InputProvider inputProvider;
    private @Mock TransformInput input;

    @BeforeEach
    void setUp() {
        lenient().when(outputProvider.output()).thenReturn(output);
        lenient().when(inputProvider.input()).thenReturn(input);
    }

    @Test
    void shouldConnectEdgesOnce() {
        builder.registerEdge(outputProvider, inputProvider);
        builder.build();
        builder.build();
        verify(output, times(1)).attachInput(input);
    }

    @Test
    void shouldMapNodes() {
        final Object operator = new Object();
        builder.registerNode("foo", operator);
        assertThat(builder.lookupNode("foo"), equalTo(operator));
    }

    @Test
    void shouldLookupOutputProvider() {
        builder.registerNode("test", outputProvider);
        assertThat(builder.lookupOutputProvider("test"), equalTo(outputProvider));
    }

    @Test
    void shouldThrowWhenOperatorNotFound() {
        assertThrows(TransformException.class, () -> builder.lookupOutputProvider("x"));
    }

    @Test
    void shouldThrowWhenOperatorNotAnOutputProvider() {
        builder.registerNode("test", new Object());
        final var ex =
                assertThrows(TransformException.class, () -> builder.lookupOutputProvider("test"));
        assertThat(ex.getMessage(), containsString("not an OutputProvider"));
    }

    @Test
    void shouldConnectEdgesCreatedDuringADeferredOperatorBuild() {
        builder.registerDeferredNode(
                "foo",
                () -> {
                    builder.registerEdge(outputProvider, inputProvider);
                    return outputProvider;
                });
        builder.build();
        verify(output, times(1)).attachInput(input);
    }

    @Test
    void shouldConnectEdgeWhenReady() {
        final var filter = filter().build();
        final var table = intIndexedTable().addFields(intField("x")).build();
        builder.registerEdgeWhenReady(filter, inputProvider);
        builder.build();
        // then
        verifyNoInteractions(input);
        // when
        table.output().attachInput(filter.input());
        // then
        final ArgumentCaptor<Schema> schemaCaptor = ArgumentCaptor.forClass(Schema.class);
        verify(input, times(1)).schemaUpdated(schemaCaptor.capture());
        assertThat(schemaCaptor.getValue().field("x"), notNullValue());
    }

    @Test
    void shouldBuildFilter() {
        final var table = builder.intIndexedTable("table").addField(intField("x")).getOrCreate();
        final var filter = builder.filter().getOrCreate();
        builder.registerEdge(table, filter);
        builder.build();
        assertThat(filter.output().schema(), notNullValue());
    }
}
