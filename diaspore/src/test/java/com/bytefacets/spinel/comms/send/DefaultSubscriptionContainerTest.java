// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.comms.send;

import static com.bytefacets.spinel.comms.send.DefaultSubscriptionContainer.defaultSubscriptionContainer;
import static com.bytefacets.spinel.schema.FieldDescriptor.intField;
import static com.bytefacets.spinel.table.IntIndexedTableBuilder.intIndexedTable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.common.Connector;
import com.bytefacets.spinel.comms.SubscriptionConfig;
import com.bytefacets.spinel.comms.subscription.ChangeDescriptorFactory;
import com.bytefacets.spinel.printer.OutputPrinter;
import com.bytefacets.spinel.schema.IntWritableField;
import com.bytefacets.spinel.table.IntIndexedTable;
import com.bytefacets.spinel.validation.Key;
import com.bytefacets.spinel.validation.RowData;
import com.bytefacets.spinel.validation.Validation;
import com.bytefacets.spinel.validation.ValidationOperator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DefaultSubscriptionContainerTest {
    private final IntIndexedTable table =
            intIndexedTable()
                    .addFields(intField("a"), intField("b"), intField("c"))
                    .includeKeyField(true)
                    .keyFieldName("key")
                    .build();
    private final ValidationOperator validation =
            new ValidationOperator(new String[] {"key"}, "a", "b", "c");
    private DefaultSubscriptionContainer container;

    @BeforeEach
    void setUp() {
        IntStream.range(0, 10)
                .forEach(
                        i -> {
                            final int row = table.beginAdd(i);
                            Stream.of("a", "b", "c")
                                    .forEach(
                                            fieldName -> {
                                                final IntWritableField field =
                                                        table.writableField(fieldName);
                                                field.setValueAt(row, i + fieldName.charAt(0));
                                            });
                            table.endAdd();
                        });
        table.fireChanges();
    }

    private void createContainer(final SubscriptionConfig config, final TransformOutput output) {
        container =
                defaultSubscriptionContainer(
                        mock(ConnectedSessionInfo.class),
                        config,
                        output,
                        ModificationHandlerRegistry.modificationHandlerRegistry());
    }

    private void init(final SubscriptionConfig config, final TransformOutput output) {
        createContainer(config, output);
        Connector.connectOutputToInput(container, OutputPrinter.printer());
        Connector.connectOutputToInput(container, validation);
    }

    @AfterEach
    void tearDown() {
        validation.assertNoActiveValidation();
    }

    @Test
    void shouldDisconnectFromSourceWhenTerminating() {
        final TransformOutput mockOutput = mock(TransformOutput.class);
        init(SubscriptionConfig.subscriptionConfig("foo").build(), mockOutput);
        container.terminateSubscription();
        verify(mockOutput, times(1)).detachInput(any());
    }

    @Nested
    class FilterTests {
        @Test
        void shouldSetUpAllPassingFilter() {
            init(SubscriptionConfig.subscriptionConfig("foo").defaultAll().build(), table.output());
            final Validation expect = validation.expect();
            expect.schema(fullSchema());
            IntStream.range(0, 10).forEach(i -> expect.added(Key.key(i), rowData(i)));
            expect.validate();
        }

        @Test
        void shouldSetUpNonePassingFilter() {
            init(
                    SubscriptionConfig.subscriptionConfig("foo").defaultNone().build(),
                    table.output());
            final Validation expect = validation.expect();
            expect.schema(fullSchema());
            expect.validate();
        }

        @Test
        void shouldModifyFilterUsingRequest() {
            init(
                    SubscriptionConfig.subscriptionConfig("foo").defaultNone().build(),
                    table.output());
            validation.clearChanges();
            container.apply(ChangeDescriptorFactory.addPredicate("key >= 2 && key < 5"));
            final Validation expect = validation.expect();
            IntStream.range(2, 5).forEach(i -> expect.added(Key.key(i), rowData(i)));
            expect.validate();
        }
    }

    @Nested
    class ProjectionTests {
        @Test
        void shouldCreateProjectionWhenFieldSelection() {
            createContainer(
                    SubscriptionConfig.subscriptionConfig("foo")
                            .setFields(List.of("b", "key"))
                            .defaultAll()
                            .build(),
                    table.output());
            final var limitedValidator = new ValidationOperator(new String[] {"key"}, "b");
            Connector.connectOutputToInput(container, limitedValidator);
            final Validation expect = limitedValidator.expect();
            expect.schema(Map.of("key", Integer.class, "b", Integer.class));
            IntStream.range(0, 10)
                    .forEach(i -> expect.added(Key.key(i), new RowData(Map.of("b", 'b' + i))));
            expect.validate();
        }
    }

    private Map<String, Class<?>> fullSchema() {
        return Map.of(
                "key", Integer.class, "a", Integer.class, "b", Integer.class, "c", Integer.class);
    }

    private RowData rowData(final int i) {
        return new RowData(Map.of("a", 'a' + i, "b", 'b' + i, "c", 'c' + i));
    }
}
