// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.jdbc.source;

import static com.bytefacets.spinel.jdbc.source.JdbcToFieldNamers.TitleCase;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class JdbcToFieldNamersTest {

    @Nested
    class TitleCaseTests {
        @ParameterizedTest
        @CsvSource({
            "_abc,Abc",
            "a car,ACar",
            "some_thing,SomeThing",
            "something,Something",
            "some-thing,SomeThing"
        })
        void shouldTransformName(final String jdbcName, final String resultName) {
            assertThat(TitleCase.jdbcToFieldName(jdbcName), equalTo(resultName));
        }

        @Test
        void shouldResetInternalStringBuilder() {
            assertThat(TitleCase.jdbcToFieldName("something"), equalTo("Something"));
            assertThat(TitleCase.jdbcToFieldName("foo"), equalTo("Foo"));
        }
    }
}
