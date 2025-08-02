// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.transform;

import static com.bytefacets.diaspore.common.Connector.connectOutputToInput;
import static java.util.Objects.requireNonNull;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.diaspore.TransformInput;
import com.bytefacets.diaspore.schema.ChangedFieldSet;
import com.bytefacets.diaspore.schema.Schema;
import javax.annotation.Nullable;

class TransformEdgeWhenReady implements TransformEdge {
    private final OutputProvider output;
    private final InputProvider input;

    TransformEdgeWhenReady(final OutputProvider output, final InputProvider input) {
        this.output = requireNonNull(output, "output");
        this.input = requireNonNull(input, "input");
    }

    @Override
    public void connect() {
        connectOutputToInput(output, new WhenSchemaReady());
    }

    private class WhenSchemaReady implements TransformInput {
        private boolean waiting = true;

        @Override
        public void schemaUpdated(@Nullable final Schema schema) {
            if (schema != null && waiting) {
                waiting = false;
                connectOutputToInput(output, input);
                output.output().detachInput(this);
            }
        }

        @Override
        public void rowsAdded(final IntIterable rows) {}

        @Override
        public void rowsChanged(final IntIterable rows, final ChangedFieldSet changedFields) {}

        @Override
        public void rowsRemoved(final IntIterable rows) {}
    }
}
