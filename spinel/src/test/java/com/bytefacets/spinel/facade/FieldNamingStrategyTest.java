// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.facade;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FieldNamingStrategyTest {

    @ParameterizedTest
    @CsvSource({
        "Field1,field_1",
        "SomeField,some_field",
        "SATScore,sat_score",
        "ScoreMAX,score_max",
        "FieldMAXSize,field_max_size",
        "FieldAVG2Size,field_avg2_size"
    })
    void shouldFormulateSnakeCase(final String input, final String expected) {
        assertThat(FieldNamingStrategy.SnakeCase.formulateName(input), equalTo(expected));
    }
}
