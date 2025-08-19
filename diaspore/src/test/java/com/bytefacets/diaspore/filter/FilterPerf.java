// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.filter;

import com.bytefacets.diaspore.PerfHarness;
import com.bytefacets.diaspore.TransformInput;
import com.bytefacets.diaspore.filter.lib.IntPredicate;
import com.bytefacets.diaspore.table.IntIndexedStructTable;
import com.bytefacets.diaspore.transform.TransformBuilder;

class FilterPerf implements PerfHarness.Callback {
    private static final int WARMUP = 100_000;
    private static final int RUN = 1_000_000;
    private IntIndexedStructTable<Input> table;
    private Input facade;

    public static void main(final String[] args) {
        new PerfHarness(WARMUP, RUN).run(new FilterPerf());
    }

    @Override
    public void init(final TransformBuilder builder, final TransformInput collector) {
        build(builder);
        table = builder.lookupNode("Input");
        final Filter filter = builder.lookupNode("Filter");
        filter.output().attachInput(collector);
        facade = table.createFacade();
    }

    @Override
    public void warmup(final int iteration) {
        iteration(iteration);
    }

    @Override
    public void step(final int iteration) {
        iteration(iteration);
    }

    private void iteration(final int i) {
        table.beginUpsert(i % 1024, facade).setValue1(i << 2).setValue2(i << 3);
        table.endUpsert();
        if (i % 32 == 0) {
            table.fireChanges();
        }
    }

    private static TransformBuilder build(final TransformBuilder builder) {
        builder.intIndexedStructTable(Input.class)
                .initialSize(1024)
                .chunkSize(256)
                .then()
                .filter("Filter")
                .initialPredicate(IntPredicate.intPredicate("Value1", i -> i % 2 == 0))
                .initialSize(1024);
        return builder;
    }

    interface Input {
        int getId();

        int getValue1();

        int getValue2();

        Input setValue1(int value);

        Input setValue2(int value);
    }
}
