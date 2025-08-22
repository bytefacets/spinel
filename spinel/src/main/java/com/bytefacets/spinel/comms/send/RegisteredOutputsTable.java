// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.comms.send;

import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.table.StringIndexedTable;
import com.bytefacets.spinel.table.StringIndexedTableBuilder;
import com.bytefacets.spinel.transform.OutputProvider;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import jakarta.annotation.Nullable;

public final class RegisteredOutputsTable implements OutputProvider, OutputRegistry {
    private final StringIndexedTable table =
            StringIndexedTableBuilder.stringIndexedTable().keyFieldName("OutputName").build();
    private final Map<String, TransformOutput> outputMap = new ConcurrentHashMap<>();

    public void register(final String name, final TransformOutput out) {
        outputMap.put(name, out);
        table.beginAdd(name);
        table.endAdd();
        table.fireChanges();
    }

    public void register(final String name, final OutputProvider out) {
        outputMap.put(name, out.output());
        table.beginAdd(name);
        table.endAdd();
        table.fireChanges();
    }

    public boolean contains(final String name) {
        return outputMap.containsKey(name);
    }

    public void readNames(final Consumer<String> consumer) {
        outputMap.keySet().forEach(consumer);
    }

    @Nullable
    @Override
    public TransformOutput lookup(final String name) {
        return outputMap.get(name);
    }

    @Override
    public TransformOutput output() {
        return table.output();
    }
}
