package com.bytefacets.diaspore.comms.send;

import static com.bytefacets.diaspore.common.Connector.connectOutputToInput;
import static com.bytefacets.diaspore.comms.send.FilterExpressionManager.filterExpressionManager;
import static com.bytefacets.diaspore.filter.FilterBuilder.filter;
import static com.bytefacets.diaspore.schema.FieldDescriptor.intField;
import static com.bytefacets.diaspore.table.IntIndexedTableBuilder.intIndexedTable;
import static com.bytefacets.diaspore.validation.Key.key;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

import com.bytefacets.diaspore.common.jexl.JexlEngineProvider;
import com.bytefacets.diaspore.comms.subscription.ChangeDescriptorFactory;
import com.bytefacets.diaspore.filter.Filter;
import com.bytefacets.diaspore.printer.OutputPrinter;
import com.bytefacets.diaspore.schema.IntWritableField;
import com.bytefacets.diaspore.table.IntIndexedTable;
import com.bytefacets.diaspore.validation.RowData;
import com.bytefacets.diaspore.validation.ValidationOperator;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FilterExpressionManagerTest {
    private final IntIndexedTable table =
            intIndexedTable()
                    .keyFieldName("key")
                    .addField(intField("a"))
                    .includeKeyField(true)
                    .build();
    private final ValidationOperator validation = new ValidationOperator(new String[] {"key"}, "a");
    private final Filter filter = filter().passesWhenNoPredicate(false).build();
    private final FilterExpressionManager manager =
            filterExpressionManager(filter, JexlEngineProvider.defaultJexlEngine(), "test");

    @BeforeEach
    void setUp() {
        connectOutputToInput(table, filter);
        connectOutputToInput(filter, OutputPrinter.printer());
        connectOutputToInput(filter, validation);
        IntStream.rangeClosed(1, 10)
                .forEach(
                        i -> {
                            final int row = table.beginAdd(i);
                            ((IntWritableField) table.writableField("a")).setValueAt(row, i * 10);
                            table.endAdd();
                        });
        table.fireChanges();
        validation.expect().schema(Map.of("key", Integer.class, "a", Integer.class)).validate();
        // no rows
    }

    @AfterEach
    void tearDown() {
        validation.assertNoActiveValidation();
    }

    @Test
    void shouldAddExpressionToFilter() {
        manager.apply(ChangeDescriptorFactory.addPredicate("key >= 2 && key < 5"));
        validation
                .expect()
                .added(key(2), rowData(2))
                .added(key(3), rowData(3))
                .added(key(4), rowData(4))
                .validate();
    }

    @Test
    void shouldCombineExpressionAsOrInFilter() {
        manager.apply(ChangeDescriptorFactory.addPredicate("key >= 2 && key < 5"));
        validation.clearChanges(); // already tested in shouldAddExpressionToFilter
        manager.apply(ChangeDescriptorFactory.addPredicate("a == 70"));
        validation.expect().added(key(7), rowData(7)).validate();
    }

    @Test
    void shouldRemoveExpressionToFilter() {
        manager.apply(ChangeDescriptorFactory.addPredicate("a == 70"));
        manager.apply(ChangeDescriptorFactory.addPredicate("key >= 2 && key < 5"));
        validation.clearChanges(); // already tested
        manager.apply(ChangeDescriptorFactory.removePredicate("a == 70"));
        validation.expect().removed(key(7)).validate();
    }

    @Test
    void shouldManageExpressionsByReferenceCount() {
        manager.apply(ChangeDescriptorFactory.addPredicate("a == 70"));
        validation.expect().added(key(7), rowData(7)).validate();
        manager.apply(ChangeDescriptorFactory.addPredicate("a == 70"));
        validation.expect().validate(); // nothing

        manager.apply(ChangeDescriptorFactory.removePredicate("a == 70"));
        validation.expect().validate(); // nothing
        manager.apply(ChangeDescriptorFactory.removePredicate("a == 70"));
        validation.expect().removed(key(7)).validate();
    }

    @Nested
    class ResponseTests {

        @Test
        void shouldReplyWithSuccessWhenAdding() {
            final ModificationResponse response =
                    manager.apply(ChangeDescriptorFactory.addPredicate("a == 70"));
            assertThat(response.success(), equalTo(true));
        }

        @Test
        void shouldReplyWithSuccessWhenRemoving() {
            manager.apply(ChangeDescriptorFactory.addPredicate("a == 70"));
            final ModificationResponse response =
                    manager.apply(ChangeDescriptorFactory.removePredicate("a == 70"));
            assertThat(response.success(), equalTo(true));
        }

        @Test
        void shouldReplyWithFailureNotFoundWhenRemovingUnknownExpression() {
            final ModificationResponse response =
                    manager.apply(ChangeDescriptorFactory.removePredicate("a == 70"));
            assertThat(response.success(), equalTo(false));
            assertThat(response.message(), containsString("Expression not found"));
            assertThat(response.message(), containsString("a == 70"));
        }
    }

    private RowData rowData(final int i) {
        return new RowData(Map.of("a", i * 10));
    }
}
