// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.groupby;

import static com.bytefacets.spinel.groupby.lib.IntGroupFunction.intGroupFunction;

import com.bytefacets.spinel.PerfHarness;
import com.bytefacets.spinel.TransformInput;
import com.bytefacets.spinel.table.IntIndexedStructTable;
import com.bytefacets.spinel.transform.TransformBuilder;

class GroupByPerf implements PerfHarness.Callback {
    private static final int WARMUP = 10_000;
    private static final int RUN = 100_000;
    private IntIndexedStructTable<Input> table;
    private Input facade;

    public static void main(final String[] args) {
        new PerfHarness(WARMUP, RUN).run(new GroupByPerf());
    }

    @Override
    public void init(final TransformBuilder builder, final TransformInput collector) {
        build(builder);
        table = builder.lookupNode("Input");
        final GroupBy gb = builder.lookupNode("Agg");
        gb.parentOutput().attachInput(collector);
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
        table.beginUpsert(i % 1024, facade)
                .setGroup(i % 64)
                .setGroupInfo(i)
                .setValue1(i << 2)
                .setValue2(i << 3);
        table.endUpsert();
        if (i % 32 == 0) {
            table.fireChanges();
        }
        if (i % 500 == 0 && i > 1024) {
            table.remove(i % 1024);
        }
    }

    private static void build(final TransformBuilder builder) {
        builder.intIndexedStructTable(Input.class)
                .initialSize(1024)
                .chunkSize(256)
                .then()
                .groupBy("Agg")
                .groupFunction(intGroupFunction("Group", 64))
                .initialInboundSize(1024)
                .includeGroupIdField("Group")
                .initialOutboundSize(64)
                .includeCountField("Count")
                .addForwardedFields("GroupInfo")
                .chunkSize(64);
    }

    interface Input {
        int getId();

        int getGroup();

        int getGroupInfo();

        int getValue1();

        int getValue2();

        Input setGroup(int value);

        Input setGroupInfo(int value);

        Input setValue1(int value);

        Input setValue2(int value);
    }
}
