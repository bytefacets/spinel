// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.common.jexl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.MapContext;
import org.junit.function.ThrowingRunnable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class JexlEngineProviderTest {
    private static final JexlEngine engine = JexlEngineProvider.defaultJexlEngine();
    private final MapContext context = new MapContext();

    @Test
    void shouldHaveAccessToMathMethods() {
        assertThat(engine.createScript("math:min(3, 4)").execute(context), equalTo(3));
        assertThat(engine.createScript("math:max(3, 4)").execute(context), equalTo(4));
        assertThat(engine.createScript("math:abs(-7)").execute(context), equalTo(7));
    }

    @Test
    void shouldHaveAccessToNow() {
        context.set("refNow", System.currentTimeMillis());
        final long diff = (Long) engine.createScript("time:now() - refNow").execute(context);
        assertThat(diff, lessThan(1000L));
    }

    @ParameterizedTest
    @CsvSource({"toLowerCase(),camelname", "toUpperCase(),CAMELNAME"})
    void shouldHaveAccessToStringMethods(final String method, final String expected) {
        context.set("name", "CamelName");
        assertThat(engine.createScript("name." + method).execute(context), equalTo(expected));
    }

    @ParameterizedTest
    @ValueSource(strings = {"java.lang.System.exit(1)"})
    void shouldReturnNull(final String source) {
        assertThat(engine.createScript(source).execute(context), nullValue());
    }

    @ParameterizedTest
    @CsvSource({
        "var result = 0,assign/modify",
        "square = (n) -> { n * n }; square(9),assign/modify",
        "i = 0; while(i < 45000) {i = i+1},global assign/modify",
        "while(( time:now() - refNow ) < 5000) System.out.print(\".\");,loop error",
    })
    void shouldFail(final String source, final String exceptionContent) {
        context.set("refNow", System.currentTimeMillis());
        final ThrowingRunnable call =
                () ->
                        System.out.printf(
                                "'%s' RESULT: %s%n",
                                source, engine.createScript(source).execute(context));
        final var ex = assertThrows(JexlException.class, call);
        assertThat(ex.getMessage(), containsString(exceptionContent));
    }
}
