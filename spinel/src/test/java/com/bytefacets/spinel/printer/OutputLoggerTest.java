// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.printer;

import static com.bytefacets.spinel.printer.OutputLoggerBuilder.logger;
import static com.bytefacets.spinel.schema.FieldDescriptor.intField;
import static com.bytefacets.spinel.table.IntIndexedTableBuilder.intIndexedTable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bytefacets.spinel.schema.IntWritableField;
import com.bytefacets.spinel.table.IntIndexedTable;
import com.bytefacets.spinel.validation.Key;
import com.bytefacets.spinel.validation.RowData;
import com.bytefacets.spinel.validation.ValidationOperator;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.event.Level;

@ExtendWith(MockitoExtension.class)
class OutputLoggerTest {
    private final IntIndexedTable table =
            intIndexedTable("table")
                    .keyFieldName("Key")
                    .addField(intField("Value1"))
                    .addField(intField("Value2"))
                    .build();
    private OutputLogger node;
    private @Mock Logger logger;
    private @Captor ArgumentCaptor<String> messageCaptor;

    @Nested
    class LoggingTests {
        @ParameterizedTest
        @EnumSource(Level.class)
        void shouldLogAtExpectedLevel(final Level level) {
            node = create(level);
            table.output().attachInput(node.input());
            verifyLogAndCapture(level, 1);
        }

        @ParameterizedTest
        @EnumSource(Level.class)
        void shouldNotLogWhenLevelIsBelowEnabledLevel(final Level level) {
            node = create(level);
            when(logger.isEnabledForLevel(level)).thenReturn(false);
            table.output().attachInput(node.input());
            verifyNoLogging();
        }

        @Test
        void shouldNotLogWhenDisabled() {
            node = create(Level.DEBUG);
            node.enabled(false);
            table.output().attachInput(node.input());
            verifyNoLogging();
        }

        @Test
        void shouldLogExpectedSchemaMessage() {
            node = create(Level.DEBUG);
            table.output().attachInput(node.input());
            verifyLogAndCapture(Level.DEBUG, 1);
            assertThat(
                    messageCaptor.getValue(),
                    equalTo(
                            "e0          SCH table: 3 fields [0,Key,Int][1,Value1,Int][2,Value2,Int]"));
        }

        @Test
        void shouldLogExpectedAddMessage() {
            node = create(Level.DEBUG);
            node.enabled(false);
            table.output().attachInput(node.input());
            node.enabled(true);

            add(1, 56);
            table.fireChanges();
            verifyLogAndCapture(Level.DEBUG, 1);
            assertThat(
                    messageCaptor.getValue(),
                    equalTo("e1          ADD r0     : [Key=1][Value1=56][Value2=0]"));
        }

        @Test
        void shouldLogExpectedChangeMessage() {
            node = create(Level.DEBUG);
            node.enabled(false);
            table.output().attachInput(node.input());
            add(1, 56);
            table.fireChanges();
            node.enabled(true);

            change(1, 57);
            table.fireChanges();
            verifyLogAndCapture(Level.DEBUG, 1);
            assertThat(messageCaptor.getValue(), equalTo("e2          CHG r0     : [Value1=57]"));
        }

        @Test
        void shouldLogExpectedRemoveMessage() {
            node = create(Level.DEBUG);
            node.enabled(false);
            table.output().attachInput(node.input());
            add(1, 56);
            table.fireChanges();
            node.enabled(true);

            table.remove(1);
            table.fireChanges();
            verifyLogAndCapture(Level.DEBUG, 1);
            assertThat(messageCaptor.getValue(), equalTo("e2          REM r0     :"));
        }

        private void verifyNoLogging() {
            verify(logger, never()).trace(any());
            verify(logger, never()).debug(any());
            verify(logger, never()).info(any());
            verify(logger, never()).warn(any());
            verify(logger, never()).error(any());
        }
    }

    @Nested
    class ForwardingTests {
        final ValidationOperator validation =
                new ValidationOperator(new String[] {"Key"}, "Value1", "Value2");

        @BeforeEach
        void setUp() {
            node = create(Level.DEBUG);
            table.output().attachInput(node.input());
            node.output().attachInput(validation.input());
        }

        @AfterEach
        void tearDown() {
            validation.assertNoActiveValidation();
        }

        @Test
        void shouldForwardSchema() {
            validation
                    .expect()
                    .schema(
                            Map.of(
                                    "Key",
                                    Integer.class,
                                    "Value1",
                                    Integer.class,
                                    "Value2",
                                    Integer.class))
                    .validate();
        }

        @Test
        void shouldForwardAdds() {
            validation.clearChanges();
            add(1, 10);
            table.fireChanges();
            validation
                    .expect()
                    .added(Key.key(1), new RowData(Map.of("Value1", 10, "Value2", 0)))
                    .validate();
        }

        @Test
        void shouldForwardChanges() {
            add(1, 10);
            add(2, 20);
            table.fireChanges();
            validation.clearChanges();
            // when
            change(1, 11);
            table.fireChanges();
            // then
            validation.expect().changed(Key.key(1), new RowData(Map.of("Value1", 11))).validate();
        }

        @Test
        void shouldForwardRemoves() {
            add(1, 10);
            add(2, 20);
            table.fireChanges();
            validation.clearChanges();
            // when
            table.remove(1);
            table.fireChanges();
            // then
            validation.expect().removed(Key.key(1)).validate();
        }
    }

    private OutputLogger create(final Level level) {
        lenient().when(logger.isEnabledForLevel(level)).thenReturn(true);
        return logger("test-logger").logLevel(level).withLogger(logger).build();
    }

    private void verifyLogAndCapture(final Level level, final int times) {
        switch (level) {
            case TRACE -> verify(logger, times(times)).trace(messageCaptor.capture());
            case DEBUG -> verify(logger, times(times)).debug(messageCaptor.capture());
            case INFO -> verify(logger, times(times)).info(messageCaptor.capture());
            case WARN -> verify(logger, times(times)).warn(messageCaptor.capture());
            case ERROR -> verify(logger, times(times)).error(messageCaptor.capture());
            default -> throw new RuntimeException("Unexpected level: " + level);
        }
    }

    private void add(final int key, final int value) {
        final int row = table.beginAdd(key);
        ((IntWritableField) table.writableField("Value1")).setValueAt(row, value);
        table.endAdd();
    }

    private void change(final int key, final int value) {
        final int row = table.beginChange(key);
        ((IntWritableField) table.writableField("Value1")).setValueAt(row, value);
        table.endChange();
    }
}
