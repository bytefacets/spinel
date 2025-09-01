// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.transform;

import static com.bytefacets.spinel.interner.IntRowInterner.intInterner;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.bytefacets.spinel.TransformInput;
import com.bytefacets.spinel.schema.FieldDescriptor;
import com.bytefacets.spinel.schema.IntWritableField;
import com.bytefacets.spinel.table.IntIndexedTable;
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
                .groupByFunction(intInterner("f1", 16))
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
                .groupByFunction(intInterner("f1", 16))
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
