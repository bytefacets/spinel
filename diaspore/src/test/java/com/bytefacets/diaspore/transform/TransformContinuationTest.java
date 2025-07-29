package com.bytefacets.diaspore.transform;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.bytefacets.diaspore.TransformInput;
import com.bytefacets.diaspore.groupby.lib.IntGroupFunction;
import com.bytefacets.diaspore.schema.FieldDescriptor;
import com.bytefacets.diaspore.schema.IntWritableField;
import com.bytefacets.diaspore.table.IntIndexedTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TransformContinuationTest {
    private final TransformBuilder transform = TransformBuilder.transform();
    private TransformContinuation continuation;
    private @Mock TransformInput input;

    @BeforeEach
    void setUp() {
        continuation =
                transform.intIndexedTable("test").addField(FieldDescriptor.intField("f1")).then();
    }

    @Test
    void shouldConnectGroupByParent() {
        continuation
                .groupBy()
                .groupFunction(IntGroupFunction.intGroupFunction("f1", 16))
                .includeCountField("count")
                .build()
                .parentOutput()
                .attachInput(input);
        validate();
    }

    @Test
    void shouldConnectGroupByChild() {
        continuation
                .groupBy()
                .groupFunction(IntGroupFunction.intGroupFunction("f1", 16))
                .includeCountField("count")
                .build()
                .parentOutput()
                .attachInput(input);
        validate();
    }

    private void validate() {
        transform.build();
        final IntIndexedTable table = transform.lookupNode("test");
        final int row = table.beginAdd(1);
        ((IntWritableField) table.writableField("f1")).setValueAt(row, 28);
        table.endAdd();
        table.fireChanges();
        verify(input, times(1)).setSource(any());
        verify(input, times(1)).schemaUpdated(any());
        verify(input, times(1)).rowsAdded(any());
    }
}
