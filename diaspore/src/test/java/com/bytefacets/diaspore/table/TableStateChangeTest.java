package com.bytefacets.diaspore.table;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.diaspore.common.InputNotifier;
import com.bytefacets.diaspore.exception.TableModificationException;
import com.bytefacets.diaspore.schema.ChangedFieldSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class TableStateChangeTest {
    private final TableStateChange state = new TableStateChange();
    private final RowConsumer consumer = new RowConsumer();

    @Nested
    class AddTests {
        @Test
        void shouldSetCurrentRowWhenAdd() {
            state.addRow(38);
            assertThat(state.currentRow(), equalTo(38));
            state.endAdd();
            assertThat(state.currentRow(), equalTo(TableRow.NO_ROW));
        }

        @Test
        void shouldAddRowWhenEndAdd() {
            IntStream.of(38, 31)
                    .forEach(
                            r -> {
                                state.addRow(r);
                                state.endAdd();
                            });
            fire();
            assertThat(consumer.added, contains(38, 31));
        }

        @Test
        void shouldThrowWhenEndAddWithoutAdd() {
            assertThrows(TableModificationException.class, state::endAdd);
        }

        @Test
        void shouldThrowWhenBeginAddWithoutClosingPreviousAdd() {
            state.addRow(38);
            assertThrows(TableModificationException.class, () -> state.addRow(1));
        }

        @Test
        void shouldThrowWhenEndChangeCalledDuringAdd() {
            state.addRow(2);
            assertThrows(TableModificationException.class, state::endChange);
        }
    }

    @Nested
    class ChangeTests {
        @Test
        void shouldSetCurrentRowWhenChange() {
            state.changeRow(38);
            assertThat(state.currentRow(), equalTo(38));
            state.endChange();
            assertThat(state.currentRow(), equalTo(TableRow.NO_ROW));
        }

        @Test
        void shouldAddRowWhenEndAdd() {
            IntStream.of(38, 31)
                    .forEach(
                            r -> {
                                state.changeRow(r);
                                state.endChange();
                            });
            fire();
            assertThat(consumer.changed, contains(38, 31));
        }

        @Test
        void shouldThrowWhenEndAddWithoutAdd() {
            assertThrows(TableModificationException.class, state::endChange);
        }

        @Test
        void shouldThrowWhenBeginAddWithoutClosingPreviousAdd() {
            state.changeRow(38);
            assertThrows(TableModificationException.class, () -> state.changeRow(1));
        }

        @Test
        void shouldThrowWhenEndAddCalledDuringChange() {
            state.changeRow(2);
            assertThrows(TableModificationException.class, state::endAdd);
        }
    }

    @Nested
    class RemoveTests {
        @Test
        void shouldNotSetCurrentRowWhenRemove() {
            state.removeRow(38);
            assertThat(state.currentRow(), equalTo(TableRow.NO_ROW));
        }

        @Test
        void shouldRegisterRemovedRow() {
            IntStream.of(38, 31).forEach(state::removeRow);
            fire();
            assertThat(consumer.removed, contains(38, 31));
        }

        @Test
        void shouldCallbackWithRowsAfterFiring() {
            IntStream.of(38, 31).forEach(state::removeRow);
            fire();
            assertThat(consumer.removedRowCleanup, contains(38, 31));
        }
    }

    @Nested
    class ChangeFieldTests {
        @Test
        void shouldRegisterChangedFieldsDuringChange() {
            state.changeRow(7);
            IntStream.of(3, 4).forEach(state::changeField);
            state.endChange();
            fire();
            assertThat(consumer.fields, containsInAnyOrder(3, 4));
        }

        @Test
        void shouldNotRegisterFieldsDuringAdd() {
            state.addRow(7);
            IntStream.of(3, 4).forEach(state::changeField);
            state.endAdd();
            fire();
            assertThat(consumer.fields, empty());
        }
    }

    @Nested
    class UpsertTests {
        @Test
        void shouldRegisterChangedRow() {
            state.changeRow(38);
            state.endUpsert();
            fire();
            assertThat(consumer.added, empty());
            assertThat(consumer.changed, contains(38));
        }

        @Test
        void shouldRegisterAddedRow() {
            state.addRow(38);
            state.endUpsert();
            fire();
            assertThat(consumer.added, contains(38));
            assertThat(consumer.changed, empty());
        }
    }

    private void fire() {
        state.fire(consumer, consumer::removedRowCleanup);
    }

    private static class RowConsumer implements InputNotifier {
        private final List<Integer> added = new ArrayList<>(4);
        private final List<Integer> changed = new ArrayList<>(4);
        private final List<Integer> removed = new ArrayList<>(4);
        private final List<Integer> removedRowCleanup = new ArrayList<>(4);
        private final Set<Integer> fields = new HashSet<>(4);

        void removedRowCleanup(final int row) {
            removedRowCleanup.add(row);
        }

        @Override
        public void notifyAdds(final IntIterable rows) {
            rows.forEach(added::add);
        }

        @Override
        public void notifyChanges(final IntIterable rows, final ChangedFieldSet changedFields) {
            rows.forEach(changed::add);
            changedFields.forEach(fields::add);
        }

        @Override
        public void notifyRemoves(final IntIterable rows) {
            rows.forEach(removed::add);
        }
    }
}
