// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.jdbc.source;

import static com.bytefacets.spinel.jdbc.source.JdbcSourceBindingProvider.jdbcSourceBindingProvider;
import static com.bytefacets.spinel.schema.MatrixStoreFieldFactory.matrixStoreFieldFactory;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.TypeId;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JdbcSourceSchemaBuilderTest {
    private @Mock ResultSetMetaData rsMetaData;
    private @Mock ResultSet rs;
    private final JdbcSourceBindingProvider provider = jdbcSourceBindingProvider();
    private JdbcSourceSchemaBuilder builder;

    @BeforeEach
    void setUp() {
        builder =
                new JdbcSourceSchemaBuilder(
                        "FRANK",
                        provider,
                        JdbcToFieldNamers.Same,
                        matrixStoreFieldFactory(10, 10, i -> {}));
    }

    @Test
    void shouldProduceSchemaFromFieldsAndTypes() throws Exception {
        setUpMetaData(mapping(Types.INTEGER, "my-int"), mapping(Types.BIGINT, "my-long"));
        //
        final Schema schema = builder.createSchema(rsMetaData);
        //
        assertThat(schema.fieldAt(0).name(), equalTo("my-int"));
        assertThat(schema.fieldAt(1).name(), equalTo("my-long"));
        assertThat(schema.fieldAt(0).typeId(), equalTo(TypeId.Int));
        assertThat(schema.fieldAt(1).typeId(), equalTo(TypeId.Long));
    }

    @Test
    void shouldProduceBindingsToFields() throws Exception {
        setUpMetaData(mapping(Types.INTEGER, "my-int"), mapping(Types.BIGINT, "my-long"));
        setUpResultSet();
        final Schema schema = builder.createSchema(rsMetaData);

        builder.bindings().get(0).process(5, rs);
        builder.bindings().get(1).process(5, rs);
        //
        verify(rs, times(1)).getInt(1);
        verify(rs, times(1)).getLong(2);
        assertThat(schema.fieldAt(0).objectValueAt(5), equalTo(46));
        assertThat(schema.fieldAt(1).objectValueAt(5), equalTo(4876487L));
    }

    private void setUpMetaData(final Mapping... mappings) throws Exception {
        when(rsMetaData.getColumnCount()).thenReturn(mappings.length);
        for (int i = 0, len = mappings.length; i < len; i++) {
            when(rsMetaData.getColumnName(i + 1)).thenReturn(mappings[i].name);
            when(rsMetaData.getColumnType(i + 1)).thenReturn(mappings[i].type);
        }
    }

    private void setUpResultSet() throws Exception {
        when(rs.getInt(anyInt())).thenReturn(46);
        when(rs.getLong(anyInt())).thenReturn(4876487L);
    }

    private Mapping mapping(final int type, final String name) {
        return new Mapping(type, name);
    }

    private record Mapping(int type, String name) {}
}
