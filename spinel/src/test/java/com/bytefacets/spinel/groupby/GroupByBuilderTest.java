// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.groupby;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

import com.bytefacets.spinel.exception.OperatorSetupException;
import com.bytefacets.spinel.schema.FieldDescriptor;
import com.bytefacets.spinel.schema.TypeId;
import org.junit.function.ThrowingRunnable;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class GroupByBuilderTest {
    private final GroupByBuilder builder = GroupByBuilder.groupBy("test");

    @Test
    void shouldThrowWhenNoFieldsDeclared() {
        final var ex = assertThrows(OperatorSetupException.class, builder::build);
        assertThat(ex.getMessage(), containsStringIgnoringCase("No fields will be in schema"));
    }

    @Nested
    class NameCollisionTests {
        @Test
        void shouldThrowWhenFieldCollidesWithCountField() {
            builder.includeCountField("foo");
            shouldThrowOnCalcField("CountFieldName");
            shouldThrowOnForwardedField("CountFieldName");
            shouldThrowOnGroupIdField("CountFieldName");
            builder.includeCountField("foo"); // allow resetting
        }

        @Test
        void shouldThrowWhenFieldCollidesWithGroupIdField() {
            builder.includeGroupIdField("foo");
            shouldThrowOnCalcField("GroupIdFieldName");
            shouldThrowOnForwardedField("GroupIdFieldName");
            shouldThrowOnCountField("GroupIdFieldName");
            builder.includeGroupIdField("foo"); // allow resetting
        }

        @Test
        void shouldThrowWhenFieldCollideWithForwardedFields() {
            builder.addForwardedFields("foo");
            shouldThrowOnCalcField("ForwardedField");
            shouldThrowOnGroupIdField("ForwardedField");
            shouldThrowOnCountField("ForwardedField");
            builder.addForwardedFields("foo"); // allow setting again
        }

        @Test
        void shouldThrowWhenFieldCollideWithCalculatedFields() {
            builder.addAggregation(funcWithOutboundName("foo"));
            shouldThrowOnForwardedField("CalculatedField");
            shouldThrowOnGroupIdField("CalculatedField");
            shouldThrowOnCountField("CalculatedField");
            builder.addAggregation(funcWithOutboundName("foo")); // allow multiple refs
        }

        private void shouldThrowOnCalcField(final String existingType) {
            validateException(
                    "CalculatedField",
                    existingType,
                    () -> builder.addAggregation(funcWithOutboundName("foo")));
        }

        private void shouldThrowOnCountField(final String existingType) {
            validateException(
                    "CountFieldName", existingType, () -> builder.includeCountField("foo"));
        }

        private void shouldThrowOnGroupIdField(final String existingType) {
            validateException(
                    "GroupIdFieldName", existingType, () -> builder.includeGroupIdField("foo"));
        }

        private void shouldThrowOnForwardedField(final String existingType) {
            validateException(
                    "ForwardedField", existingType, () -> builder.addForwardedFields("foo"));
        }

        private void validateException(
                final String type1, final String type2, final ThrowingRunnable action) {
            final var ex = assertThrows(OperatorSetupException.class, action);
            assertThat(ex.getMessage(), containsString("'foo'"));
            assertThat(ex.getMessage(), containsString(type1));
            assertThat(ex.getMessage(), containsString(type2));
        }
    }

    private AggregationFunction funcWithOutboundName(final String name) {
        final var func = mock(AggregationFunction.class);
        lenient()
                .doAnswer(
                        invocation -> {
                            invocation
                                    .getArgument(0, AggregationSetupVisitor.class)
                                    .addOutboundField(new FieldDescriptor(TypeId.Int, name, null));
                            return null;
                        })
                .when(func)
                .collectFieldReferences(any());
        return func;
    }
}
