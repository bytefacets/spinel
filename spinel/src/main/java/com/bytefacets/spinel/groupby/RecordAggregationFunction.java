// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.groupby;

import static com.bytefacets.spinel.facade.DefaultValueImplFactory.defaultValueImplFactory;
import static com.bytefacets.spinel.facade.StructFacadeFactory.structFacadeFactory;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.spinel.common.EventType;
import com.bytefacets.spinel.facade.DefaultValueImplFactory;
import com.bytefacets.spinel.facade.StructFacade;
import com.bytefacets.spinel.facade.StructFacadeFactory;
import com.bytefacets.spinel.facade.StructFieldExtractor;
import com.bytefacets.spinel.schema.FieldResolver;
import com.bytefacets.spinel.schema.SchemaBindable;

/**
 * Allows the use of typed facades to implement an aggregation function. The &lt;INPUT&gt; type
 * represents the input data and &lt;OUTPUT&gt; represents the output data.
 *
 * <p>The given Accumulator will be called back for all operations: Add, Change, and Remove for the
 * group. For Adds, the `oldValue` argument will only return the default values for the getters on
 * the interface. For Removes, the `newValue` will only return default values for the getters.
 *
 * <pre>
 * interface Scores {
 *     int getExam1Score();
 *     int getExam2Score();
 *     int getPaperScore();
 * }
 * interface FinalGrade {
 *     int getTotalScore();
 *     void setTotalScore(int value);
 * }
 * RecordAggregationFunction.Accumulator&lt;Scores,FinalGrade&gt; accumulator =
 *     new RecordAggregationFunction.Accumulator&lt;&gt;() {
 *          void accumulate(EventType eventType, FinalGrade finalGrade, Scores prev, Scores current) {
 *              int delta1Score = current.getExam1Score() - prev.getExam1Score();
 *              int delta2Score = current.getExam2Score() - prev.getExam2Score();
 *              int deltaPaperScore = current.getPaperScore() - prev.getPaperScore();
 *              int deltaForRow = delta1Score + delta2Score + deltaPaperScore;
 *              finalGrade.setTotalScore(finalGrade.getTotalScore() + deltaForRow);
 *          }
 *     };
 *
 * RecordAggregationFunction&lt;Scores,FinalGrade&gt function =
 *      recordAggregationFunction(Scores.class, FinalGrades.class, accumulator);
 * </pre>
 *
 * @param <INPUT> the input type which should only have getters
 * @param <OUTPUT> the output type which should have getters (to read the current group value) and
 *     setters (to update the group value)
 */
public final class RecordAggregationFunction<INPUT, OUTPUT> implements AggregationFunction {
    private final Class<INPUT> inputType;
    private final Class<OUTPUT> outputType;
    private final Accumulator<INPUT, OUTPUT> accumulator;
    private final INPUT defaultInputValue;
    private final INPUT previousFacade;
    private final INPUT inputFacade;
    private final OUTPUT outputFacade;
    private final FacadeMover<INPUT, OUTPUT> mover;

    /**
     * User implementation of the aggregation logic.
     *
     * @param <INPUT> the input type which should only have getters
     * @param <OUTPUT> the output type which should have optional getters (to read the current group
     *     value) and setters (to update the group value)
     */
    public interface Accumulator<INPUT, OUTPUT> {
        /**
         * User-supplied callback for accumulation logic.
         *
         * @param eventType the input event being processed
         * @param currentGroupValue a facade for accessing the current group value and setting new
         *     values
         * @param oldValue a facade for accessing the previous value of the input row. During an add
         *     this will always return the default value for the data type
         * @param newValue a facade for accessing the new value of the input row. During a remove,
         *     this will always return the default value for the data type
         */
        void accumulate(
                EventType eventType, OUTPUT currentGroupValue, INPUT oldValue, INPUT newValue);
    }

    public static <INPUT, OUTPUT>
            RecordAggregationFunction<INPUT, OUTPUT> recordAggregationFunction(
                    final Class<INPUT> inputType,
                    final Class<OUTPUT> outputType,
                    final Accumulator<INPUT, OUTPUT> accumulator) {
        return new RecordAggregationFunction<>(
                inputType,
                outputType,
                accumulator,
                structFacadeFactory(),
                defaultValueImplFactory());
    }

    RecordAggregationFunction(
            final Class<INPUT> inputType,
            final Class<OUTPUT> outputType,
            final Accumulator<INPUT, OUTPUT> accumulator,
            final StructFacadeFactory factory,
            final DefaultValueImplFactory defaultValueFactory) {
        this.inputType = inputType;
        this.outputType = outputType;
        this.accumulator = accumulator;
        this.previousFacade = factory.createFacade(inputType);
        this.inputFacade = factory.createFacade(inputType);
        this.outputFacade = factory.createFacade(outputType);
        this.defaultInputValue = defaultValueFactory.createDefaultValue(inputType);
        this.mover = new FacadeMover<>(outputFacade, previousFacade, inputFacade);
    }

    @Override
    public void collectFieldReferences(final AggregationSetupVisitor visitor) {
        StructFieldExtractor.consumeFields(
                inputType,
                writable -> {},
                readable -> {
                    visitor.addInboundField(readable.name());
                    visitor.addPreviousValueField(readable.name());
                });
        StructFieldExtractor.consumeFields(outputType, visitor::addOutboundField, readable -> {});
    }

    @Override
    public void bindToSchema(
            final FieldResolver previousResolver,
            final FieldResolver currentResolver,
            final FieldResolver outboundResolver) {
        ((SchemaBindable) previousFacade).bindToSchema(previousResolver);
        ((SchemaBindable) inputFacade).bindToSchema(currentResolver);
        ((SchemaBindable) outputFacade).bindToSchema(outboundResolver);
    }

    @Override
    public void unbindSchema() {
        ((SchemaBindable) previousFacade).unbindSchema();
        ((SchemaBindable) inputFacade).unbindSchema();
        ((SchemaBindable) outputFacade).unbindSchema();
    }

    @Override
    public void groupRowsAdded(final int group, final IntIterable rows) {
        mover.moveOutput(group);
        rows.forEach(this::processAdd);
    }

    @Override
    public void groupRowsChanged(final int group, final IntIterable rows) {
        mover.moveOutput(group);
        rows.forEach(this::processChange);
    }

    @Override
    public void groupRowsRemoved(final int group, final IntIterable rows) {
        mover.moveOutput(group);
        rows.forEach(this::processRemove);
    }

    private void processAdd(final int row) {
        mover.moveInputs(row);
        accumulator.accumulate(EventType.add, outputFacade, defaultInputValue, inputFacade);
    }

    private void processChange(final int row) {
        mover.moveInputs(row);
        accumulator.accumulate(EventType.change, outputFacade, previousFacade, inputFacade);
    }

    private void processRemove(final int row) {
        mover.moveInputs(row);
        accumulator.accumulate(EventType.remove, outputFacade, previousFacade, defaultInputValue);
    }

    private static final class FacadeMover<INPUT, OUTPUT> {
        final StructFacade input;
        final StructFacade prev;
        final StructFacade output;

        FacadeMover(final OUTPUT output, final INPUT prev, final INPUT input) {
            this.input = (StructFacade) input;
            this.prev = (StructFacade) prev;
            this.output = (StructFacade) output;
        }

        private void moveOutput(final int row) {
            output.moveToRow(row);
        }

        private void moveInputs(final int row) {
            input.moveToRow(row);
            prev.moveToRow(row);
        }
    }
}
