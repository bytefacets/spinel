// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.jdbc.source;

import static com.bytefacets.spinel.jdbc.source.JdbcSourceBuilder.jdbcSource;
import static com.bytefacets.spinel.jdbc.source.JdbcUtil.mapping;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.bytefacets.spinel.common.Connector;
import com.bytefacets.spinel.printer.OutputLoggerBuilder;
import com.bytefacets.spinel.validation.Key;
import com.bytefacets.spinel.validation.RowData;
import com.bytefacets.spinel.validation.ValidationOperator;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.event.Level;

@ExtendWith(MockitoExtension.class)
class JdbcSourceTest {
    private @Mock ResultSetMetaData resultSetMetaData;
    private @Mock ResultSet resultSet;
    private final ValidationOperator validator =
            new ValidationOperator(new String[] {"Id"}, "Val1", "Val2");
    private final RowData.RowDataTemplate values = RowData.template("Val1", "Val2");
    private JdbcSource source;

    @Test
    void shouldProcessResultSetToOutput() throws Exception {
        mockMetaData(
                mapping(Types.INTEGER, "id"),
                mapping(Types.BIGINT, "val_1"),
                mapping(Types.VARCHAR, "val_2"));
        mockResultSet(3, 12);
        build();
        source.process(resultSet);
        //
        validator
                .expect()
                .schema(Map.of("Id", Integer.class, "Val1", Long.class, "Val2", String.class))
                .added(Key.key(25), values.rowData(29L, "dd"))
                .added(Key.key(13), values.rowData(17L, "d1"))
                .added(Key.key(1), values.rowData(5L, "c5"))
                .validate();
    }

    @AfterEach
    void tearDown() {
        validator.assertNoActiveValidation();
    }

    void build() {
        source = jdbcSource("test").batchSize(2).chunkSize(2).initialSize(2).getOrCreate();
        Connector.connectInputToOutput(
                OutputLoggerBuilder.logger("TEST").logLevel(Level.INFO).build(), source);
        Connector.connectInputToOutput(validator, source);
    }

    @SuppressWarnings("IntegerMultiplicationImplicitCastToLong")
    private void mockResultSet(final int rowCt, final int salt) throws Exception {
        when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        final AtomicInteger remainingRowCt = new AtomicInteger(rowCt);
        doAnswer(inv -> remainingRowCt.getAndDecrement() > 0).when(resultSet).next();
        doAnswer(inv -> remainingRowCt.get() * salt + 1).when(resultSet).getInt(anyInt());
        doAnswer(inv -> (long) (remainingRowCt.get() * salt) + 5L)
                .when(resultSet)
                .getLong(anyInt());
        doAnswer(inv -> Integer.toString(remainingRowCt.get() * salt + 197, 16))
                .when(resultSet)
                .getString(anyInt());
    }

    private void mockMetaData(final JdbcUtil.Mapping... mappings) throws Exception {
        when(resultSetMetaData.getColumnCount()).thenReturn(mappings.length);
        for (int i = 0; i < mappings.length; i++) {
            when(resultSetMetaData.getColumnName(i + 1)).thenReturn(mappings[i].name());
            when(resultSetMetaData.getColumnType(i + 1)).thenReturn(mappings[i].type());
        }
    }
}
