// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.groupby;

import static com.bytefacets.spinel.schema.Metadata.metadata;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.bytefacets.spinel.exception.FieldNotFoundException;
import com.bytefacets.spinel.interner.RowInterner;
import com.bytefacets.spinel.schema.Field;
import com.bytefacets.spinel.schema.FieldBitSet;
import com.bytefacets.spinel.schema.FieldDescriptor;
import com.bytefacets.spinel.schema.FieldList;
import com.bytefacets.spinel.schema.FieldResolver;
import com.bytefacets.spinel.schema.IntField;
import com.bytefacets.spinel.schema.RowIdentityField;
import com.bytefacets.spinel.schema.Schema;
import com.bytefacets.spinel.schema.TypeId;
import com.bytefacets.spinel.schema.WritableField;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class GroupBySchemaBuilderTest {
    private final GroupMapping groupMapping = new GroupMapping(2, 2);
    private @Mock RowInterner groupFunction;
    private DependencyMap depMap;

    @Nested
    class GroupFunctionFieldTests {
        private Schema inSchema;
        private Schema parentSchema;

        @BeforeEach
        void setUp() {
            inSchema = schema("i1", "i2", "f1", "g1", "f2", "g2", "f3", "g3");
        }

        @Test
        void shouldAddGroupFunctionFieldsToParentSchema() {
            doAnswer(
                            inv -> {
                                final var resolver = inv.getArgument(0, FieldResolver.class);
                                Stream.of("i1", "i2").forEach(resolver::findIntField);
                                return null;
                            })
                    .when(groupFunction)
                    .bindToSchema(any());
            final var builder =
                    new GroupBySchemaBuilder(
                            "foo",
                            null,
                            null,
                            2,
                            2,
                            List.of(),
                            new LinkedHashSet<>(List.of("g1", "g2", "g3")));
            parentSchema = builder.buildParentSchema(inSchema, groupFunction, groupMapping);
            assertThat(parentSchema.field("i1"), notNullValue());
            assertThat(parentSchema.field("i2"), notNullValue());
        }

        @Test
        void shouldAccommodateFieldsFromGroupFunctionAndForward() {
            doAnswer(
                            inv -> {
                                final var resolver = inv.getArgument(0, FieldResolver.class);
                                Stream.of("i1", "i2").forEach(resolver::findIntField);
                                return null;
                            })
                    .when(groupFunction)
                    .bindToSchema(any());
            final var builder =
                    new GroupBySchemaBuilder(
                            "foo",
                            null,
                            null,
                            2,
                            2,
                            List.of(),
                            new LinkedHashSet<>(List.of("g1", "i2")));
            parentSchema = builder.buildParentSchema(inSchema, groupFunction, groupMapping);
            assertThat(parentSchema.field("i1"), notNullValue());
            assertThat(parentSchema.field("i2"), notNullValue());
            assertThat(parentSchema.field("g1"), notNullValue());
        }
    }

    @Nested
    class GroupFieldsTests {
        private Schema inSchema;
        private Schema parentSchema;

        @BeforeEach
        void setUp() {
            inSchema = schema("f1", "g1", "f2", "g2", "f3", "g3");
            final var builder =
                    new GroupBySchemaBuilder(
                            "foo",
                            null,
                            null,
                            2,
                            2,
                            List.of(),
                            new LinkedHashSet<>(List.of("g1", "g2", "g3")));
            parentSchema = builder.buildParentSchema(inSchema, groupFunction, groupMapping);
            depMap = builder.dependencyMap();
        }

        @Test
        void shouldAddGroupFieldsToParentSchema() {
            assertThat(parentSchema.size(), equalTo(3));
            Stream.of("g1", "g2", "g3")
                    .forEach(
                            expectedName ->
                                    assertThat(parentSchema.field(expectedName), notNullValue()));
        }

        @ParameterizedTest
        @ValueSource(strings = {"g1", "g2", "g3"})
        void shouldMapGroupFieldDependencies(final String groupField) {
            final BitSet inboundFieldIds = new BitSet();
            // these should all be dropped in the translation
            Stream.of("f1", "f2", "f3")
                    .forEach(
                            notGroupField ->
                                    inboundFieldIds.set(inSchema.field(notGroupField).fieldId()));
            // this is the isolated test field
            inboundFieldIds.set(inSchema.field(groupField).fieldId());

            depMap.translateInboundChangeFields(FieldBitSet.fieldBitSet(inboundFieldIds));
            final var outChanges = depMap.outboundFieldChangeSet();
            assertThat(outChanges.size(), equalTo(1));
            assertThat(
                    outChanges.isChanged(parentSchema.field(groupField).fieldId()), equalTo(true));
        }

        @Test
        void shouldThrowWhenGroupFieldNotFound() {
            final var builder =
                    new GroupBySchemaBuilder(
                            "foo", null, null, 2, 2, List.of(), Set.of("not-around"));
            final var ex =
                    assertThrows(
                            FieldNotFoundException.class,
                            () -> builder.buildParentSchema(inSchema, groupFunction, groupMapping));
            assertThat(ex.getMessage(), containsString("'not-around'"));
            assertThat(ex.getMessage(), containsString("'in-schema'"));
        }
    }

    @Nested
    class CalcFieldTests {
        private Schema inSchema;
        private Schema parentSchema;
        private @Mock(strictness = Mock.Strictness.LENIENT) AggregationFunction calc1;
        private @Mock(strictness = Mock.Strictness.LENIENT) AggregationFunction calc2;
        private @Mock(strictness = Mock.Strictness.LENIENT) AggregationFunction calc3;
        private List<AggregationFunction> calcFields;
        private GroupBySchemaBuilder builder;

        private AggregationFunction calc(
                final AggregationFunction calc, final String name, final String... inputDeps) {
            doAnswer(
                            invocation -> {
                                final var visitor =
                                        invocation.getArgument(0, AggregationSetupVisitor.class);
                                visitor.addOutboundField(
                                        new FieldDescriptor(
                                                TypeId.Int, name, metadata(Set.of("is-" + name))));
                                for (String in : inputDeps) {
                                    visitor.addInboundField(in);
                                    visitor.addPreviousValueField(in);
                                }
                                return null;
                            })
                    .when(calc)
                    .collectFieldReferences(any());
            doAnswer(
                            invocation -> {
                                final var prevResolver =
                                        invocation.getArgument(0, FieldResolver.class);
                                final var curResolver =
                                        invocation.getArgument(1, FieldResolver.class);
                                final var outResolver =
                                        invocation.getArgument(2, FieldResolver.class);
                                for (String in : inputDeps) {
                                    curResolver.getField(in);
                                    prevResolver.getField(in);
                                }
                                outResolver.getField(name);
                                return null;
                            })
                    .when(calc)
                    .bindToSchema(any(), any(), any());

            return calc;
        }

        @BeforeEach
        void setUp() {
            calcFields =
                    List.of(
                            calc(calc1, "c1", "i1"),
                            calc(calc2, "c2", "i1", "i2"),
                            calc(calc3, "c3", "i3"));
            inSchema = schema("f1", "i1", "f2", "i2", "f3", "i3");
            builder = new GroupBySchemaBuilder("foo", null, null, 2, 2, calcFields, Set.of());
            parentSchema = builder.buildParentSchema(inSchema, groupFunction, groupMapping);
            depMap = builder.dependencyMap();
        }

        @Test
        void shouldAddCalcFieldsToParentSchema() {
            assertThat(parentSchema.size(), equalTo(3));
            Stream.of("c1", "c2", "c3")
                    .forEach(
                            expectedName -> {
                                assertThat(
                                        parentSchema.field(expectedName).field(),
                                        instanceOf(WritableField.class));
                                assertThat(
                                        parentSchema.field(expectedName).metadata(),
                                        equalTo(metadata(Set.of("is-" + expectedName))));
                            });
        }

        @Test
        void shouldBindCalcFieldsWithFieldResolver() {
            assertThat(parentSchema.size(), equalTo(3));
            Stream.of(calc1, calc2, calc3)
                    .forEach(calc -> verify(calc, times(1)).bindToSchema(any(), any(), any()));
        }

        @Test
        void shouldUnbindCalcFieldsWithFieldResolver() {
            builder.unbindCalculatedFields();
            Stream.of(calc1, calc2, calc3).forEach(calc -> verify(calc, times(1)).unbindSchema());
        }

        @ParameterizedTest
        @CsvSource({"i1,c1|c2", "i2,c2", "i3,c3", "i1|i2,c1|c2"})
        void shouldMapCalcDependencies(final String inChange, final String outChanges) {
            final BitSet inboundFieldIds = new BitSet();
            // these should all be dropped in the translation
            Stream.of("f1", "f2", "f3")
                    .forEach(
                            notGroupField ->
                                    inboundFieldIds.set(inSchema.field(notGroupField).fieldId()));
            // this is the isolated test field
            Stream.of(inChange.split("\\|"))
                    .forEach(inField -> inboundFieldIds.set(inSchema.field(inField).fieldId()));
            final var triggeredFunctions =
                    depMap.translateInboundChangeFields(FieldBitSet.fieldBitSet(inboundFieldIds));
            final var expectedOut = outChanges.split("\\|");
            final Map<String, AggregationFunction> funcMap =
                    Map.of("c1", calc1, "c2", calc2, "c3", calc3);
            assertThat(triggeredFunctions.size(), equalTo(expectedOut.length));
            for (String expected : expectedOut) {
                assertThat(triggeredFunctions.containsKey(funcMap.get(expected)), equalTo(true));
            }
        }

        @Test
        void shouldThrowWhenFunctionDoesNotFindField() {
            final var badSchema = schema("f1", "i1", "f2", "i2", "f3"); // no i3
            final var ex =
                    assertThrows(
                            FieldNotFoundException.class,
                            () ->
                                    builder.buildParentSchema(
                                            badSchema, groupFunction, groupMapping));
            assertThat(ex.getMessage(), containsString("'i3'"));
            assertThat(ex.getMessage(), containsString("'in-schema'"));
        }
    }

    @Nested
    class GroupIdFieldTests {
        private Schema inSchema;
        private Schema parentSchema;

        @BeforeEach
        void setUp() {
            inSchema = schema("f1", "g1", "f2", "g2", "f3", "g3");
            final var builder =
                    new GroupBySchemaBuilder("foo", "group-id", null, 2, 2, List.of(), Set.of());
            parentSchema = builder.buildParentSchema(inSchema, groupFunction, groupMapping);
            depMap = builder.dependencyMap();
        }

        @Test
        void shouldAddGroupIdFieldToParentSchema() {
            assertThat(parentSchema.size(), equalTo(1));
            assertThat(parentSchema.field("group-id"), notNullValue());
            assertThat(parentSchema.field("group-id").field(), instanceOf(RowIdentityField.class));
            assertThat(parentSchema.field("group-id").fieldId(), equalTo(0));
            assertThat(parentSchema.field("group-id").fieldId(), equalTo(depMap.groupFieldId()));
            assertThat(depMap.groupFieldId(), equalTo(0));
        }

        @Test
        void shouldNotSetDependencyMapGroupIdFieldIdWhenNoGroupIdField() {
            depMap.reset();
            new GroupBySchemaBuilder("foo", null, null, 2, 2, List.of(), Set.of("f1"))
                    .buildParentSchema(inSchema, groupFunction, groupMapping);
            assertThat(depMap.groupFieldId(), equalTo(-1));
        }
    }

    @Nested
    class CountFieldTests {
        private Schema inSchema;
        private Schema parentSchema;

        @BeforeEach
        void setUp() {
            inSchema = schema("f1", "g1", "f2", "g2", "f3", "g3");
            final var builder =
                    new GroupBySchemaBuilder("foo", null, "count", 2, 2, List.of(), Set.of());
            parentSchema = builder.buildParentSchema(inSchema, groupFunction, groupMapping);
            depMap = builder.dependencyMap();
        }

        @Test
        void shouldAddCountFieldToParentSchema() {
            assertThat(parentSchema.size(), equalTo(1));
            assertThat(parentSchema.field("count"), notNullValue());
            assertThat(parentSchema.field("count").fieldId(), equalTo(0));
            depMap.markCountChanged();
            assertThat(depMap.outboundFieldChangeSet().isChanged(0), equalTo(true));
        }

        @Test
        void shouldNotSetDependencyMapCountFieldIdWhenNoGroupIdField() {
            depMap.reset();
            new GroupBySchemaBuilder("foo", null, null, 2, 2, List.of(), Set.of("f1"))
                    .buildParentSchema(inSchema, groupFunction, groupMapping);
            depMap.markCountChanged();
            assertThat(depMap.outboundFieldChangeSet().size(), equalTo(0));
        }
    }

    // UPCOMING: CountFieldTests

    private Schema schema(final String... fields) {
        final Map<String, Field> map = new LinkedHashMap<>();
        Stream.of(fields).forEach(name -> map.put(name, intField()));
        return Schema.schema("in-schema", FieldList.fieldList(map));
    }

    private IntField intField() {
        final IntField field = mock(IntField.class);
        lenient().when(field.typeId()).thenReturn(TypeId.Int);
        return field;
    }
}
