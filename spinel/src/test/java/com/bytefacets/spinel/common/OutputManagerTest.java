// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.common;

import static com.bytefacets.spinel.common.BitSetRowProvider.bitSetRowProvider;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.collections.vector.IntVector;
import com.bytefacets.spinel.TransformInput;
import com.bytefacets.spinel.schema.ChangedFieldSet;
import com.bytefacets.spinel.schema.FieldList;
import com.bytefacets.spinel.schema.IntField;
import com.bytefacets.spinel.schema.Schema;
import java.util.BitSet;
import java.util.Map;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;

class OutputManagerTest {
    private final BitSet rows = new BitSet();
    private final OutputManager manager = OutputManager.outputManager(bitSetRowProvider(rows));
    private final Schema schema =
            Schema.schema("foo", FieldList.fieldList(Map.of("f1", (IntField) row -> 0)));

    @Test
    void shouldAllowReEntrantOnAdd() {
        final InputShell trigger = new InputShell();
        trigger.onAdd = () -> manager.addInput(new InputShell());
        manager.addInput(trigger);
        manager.updateSchema(schema);
        manager.notifyAdds(new IntVector(2));
    }

    @Test
    void shouldAllowReEntrantOnSchema() {
        final InputShell trigger = new InputShell();
        trigger.onSchema = () -> manager.addInput(new InputShell());
        manager.addInput(trigger);
        manager.updateSchema(schema);
    }

    @Test
    void shouldAllowReEntrantOnChange() {
        final InputShell trigger = new InputShell();
        trigger.onChange = () -> manager.addInput(new InputShell());
        manager.addInput(trigger);
        manager.updateSchema(schema);
        manager.notifyChanges(new IntVector(2), mock(ChangedFieldSet.class));
    }

    @Test
    void shouldAllowReEntrantOnRemove() {
        final InputShell trigger = new InputShell();
        trigger.onRemove = () -> manager.addInput(new InputShell());
        manager.addInput(trigger);
        manager.updateSchema(schema);
        manager.notifyRemoves(new IntVector(2));
    }

    @Test
    void shouldApplySchemaToInput() {
        final InputShell trigger = new InputShell();
        manager.addInput(trigger);
        manager.updateSchema(schema);
        assertThat(trigger.received, equalTo(schema));
        manager.updateSchema(null);
        assertThat(trigger.received, nullValue());
    }

    private static class InputShell implements TransformInput {
        private Schema received;
        private Runnable onSchema;
        private Runnable onAdd;
        private Runnable onChange;
        private Runnable onRemove;

        @SuppressWarnings("NeedBraces")
        private void runIfNonNull(final Runnable runnable) {
            if (runnable != null) runnable.run();
        }

        @Override
        public void schemaUpdated(@Nullable final Schema schema) {
            this.received = schema;
            runIfNonNull(onSchema);
        }

        @Override
        public void rowsAdded(final IntIterable rows) {
            runIfNonNull(onAdd);
        }

        @Override
        public void rowsChanged(final IntIterable rows, final ChangedFieldSet changedFields) {
            runIfNonNull(onChange);
        }

        @Override
        public void rowsRemoved(final IntIterable rows) {
            runIfNonNull(onRemove);
        }
    }
}
