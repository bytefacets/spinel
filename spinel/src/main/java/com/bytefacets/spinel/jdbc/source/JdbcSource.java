// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.jdbc.source;

import static com.bytefacets.spinel.common.BitSetRowProvider.bitSetRowProvider;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.vector.IntVector;
import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.common.OutputManager;
import com.bytefacets.spinel.exception.OperatorSetupException;
import com.bytefacets.spinel.transform.OutputProvider;
import java.sql.ResultSet;
import java.util.BitSet;
import java.util.List;

/**
 * Best used for loading a static source. A call to {@link JdbcSource#process} appends the data to
 * the output. Note that due to threading, you should probably do this on startup and not while the
 * app is running unless the blocking nature of the ResultSet iteration is not a concern.
 */
public final class JdbcSource implements OutputProvider {
    private final JdbcSourceSchemaBuilder schemaBuilder;
    private final OutputManager outputManager;
    private final BitSet activeRows = new BitSet();
    private final int batchSize;
    private final IntVector addedRows;
    private int nextRow = 0;

    JdbcSource(final JdbcSourceSchemaBuilder schemaBuilder, final int batchSize) {
        this.schemaBuilder = requireNonNull(schemaBuilder, "schemaBuilder");
        this.outputManager = OutputManager.outputManager(bitSetRowProvider(activeRows));
        this.batchSize = batchSize;
        this.addedRows = new IntVector(batchSize);
        if (batchSize <= 0) {
            throw new OperatorSetupException("BatchSize must be > 0, but was " + batchSize);
        }
    }

    /**
     * Appends the contents of this ResultSet to the output. Be careful of the threading. This is
     * probably best used at startup, before starting other data ingest threads or opening to
     * subscriptions, unless you are ok with the processing of the ResultSet on the data thread.
     */
    public void process(final ResultSet rs) throws Exception {
        if (outputManager.schema() == null) {
            outputManager.updateSchema(schemaBuilder.createSchema(rs.getMetaData()));
        }
        final List<ResultSetBinding> bindings = schemaBuilder.bindings();
        while (rs.next()) {
            final int row = nextRow++;
            for (var binding : bindings) {
                binding.process(row, rs);
            }
            activeRows.set(row);
            addedRows.append(row);
            if (addedRows.size() == batchSize) {
                fireAdds();
            }
        }
        if (!addedRows.isEmpty()) {
            fireAdds();
        }
    }

    private void fireAdds() {
        outputManager.notifyAdds(addedRows);
        addedRows.clear();
    }

    @Override
    public TransformOutput output() {
        return outputManager.output();
    }
}
