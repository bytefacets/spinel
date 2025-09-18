// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.examples.printer;

import com.bytefacets.collections.types.LongType;
import com.bytefacets.collections.types.ShortType;
import com.bytefacets.spinel.common.Connector;
import com.bytefacets.spinel.groupby.GroupBy;
import com.bytefacets.spinel.groupby.GroupByBuilder;
import com.bytefacets.spinel.groupby.lib.SumFactory;
import com.bytefacets.spinel.printer.OutputPrinter;
import com.bytefacets.spinel.schema.AttributeConstants;
import com.bytefacets.spinel.schema.DisplayMetadata;
import com.bytefacets.spinel.schema.Metadata;
import com.bytefacets.spinel.schema.ValueMetadata;
import com.bytefacets.spinel.table.IntIndexedStructTable;
import com.bytefacets.spinel.table.IntIndexedStructTableBuilder;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * This example demonstrates the use of field metadata as it pertains to logging and printing
 * operators.
 *
 * <ul>
 *   <li>The example starts by creating a table using an interface to describe the fields and their
 *       types. Note that there are annotations describing two numeric fields as having "Text"
 *       content.
 *   <li>Next the table is connected to two different aggregations
 *   <li>The three outputs (table and two aggregations) are each connected to a printer
 *   <li>Example data is added to the table
 *   <li>An example change is made to demonstrate the change behavior in the table and group by
 *       operators
 * </ul>
 */
public final class PrinterExample {
    private static final byte[] temp = new byte[8];
    private static final String[] countryCodes = new String[] {"US", "GB", "DE", "FR", "JP", "BR"};
    private static final String[] dataCenters =
            new String[] {"useast1", "uswest2", "euwest1", "apne1", "saeast1"};

    private PrinterExample() {}

    public static void main(final String[] args) {
        final IntIndexedStructTable<DataModel> table =
                IntIndexedStructTableBuilder.intIndexedStructTable(DataModel.class).build();

        // note that AggregationFunctions like Sum cannot be shared
        final GroupBy countryAggregation =
                GroupByBuilder.groupBy("ByCountry")
                        .groupByFields("CountryCode")
                        .includeCountField("Count")
                        .addAggregation(
                                SumFactory.sumToDouble("Amount", "TotalAmount", amountMetadata()))
                        .build();
        final GroupBy dataCenterAggregation =
                GroupByBuilder.groupBy("ByDataCenter")
                        .groupByFields("DataCenter")
                        .includeCountField("Count")
                        .addAggregation(
                                SumFactory.sumToDouble("Amount", "TotalAmount", amountMetadata()))
                        .build();

        // connect printers to the three outputs
        // connecting these first so the table is printed before the aggregations
        Connector.connectInputToOutput(OutputPrinter.printer(), table);
        Connector.connectInputToOutput(OutputPrinter.printer(), countryAggregation);
        Connector.connectInputToOutput(OutputPrinter.printer(), dataCenterAggregation);

        // connect two different aggregations to the table
        Connector.connectInputToOutput(countryAggregation, table);
        Connector.connectInputToOutput(dataCenterAggregation, table);

        // put some mock data and fire it to show the results in the printers
        final Random random = new Random(37653763);
        final DataModel facade = table.createFacade();
        for (int i = 0; i < 21; i++) {
            table.beginAdd(i, facade)
                    .setCountryCode(toShort(countryCodes[i % countryCodes.length]))
                    .setDate(2025_09_17)
                    .setAmount(random.nextDouble(5000, 10000))
                    .setProduct("product" + (i % 6))
                    .setDataCenter(toLong(dataCenters[i % dataCenters.length]))
                    .setTimestamp(currentNanos());
            table.endAdd();
        }
        table.fireChanges();

        // example change here
        table.beginChange(5, facade).setAmount(7893.7767);
        table.endChange();
        table.fireChanges();
    }

    /** Converts the string value to a short by encoding it as LittleEndian. */
    private static short toShort(final String value) {
        final int len = Math.min(2, value.length());
        for (int i = 0; i < len; i++) {
            temp[i] = (byte) value.charAt(i);
        }
        // padding if string is shorter than 2
        for (int i = len; i < 2; i++) {
            temp[i] = ' ';
        }
        return ShortType.readLE(temp, 0);
    }

    /** Converts the string value to a long by encoding it as LittleEndian. */
    private static long toLong(final String value) {
        final int len = Math.min(8, value.length());
        for (int i = 0; i < len; i++) {
            temp[i] = (byte) value.charAt(i);
        }
        // padding if string is shorter than 8
        for (int i = len; i < 8; i++) {
            temp[i] = ' ';
        }
        return LongType.readLE(temp, 0);
    }

    private static long currentNanos() {
        final Instant now = Instant.now();
        return TimeUnit.SECONDS.toNanos(now.getEpochSecond()) + now.getNano();
    }

    // in the output, you'll see that the table shows 4 decimal places
    // but th aggregation output shows no decimal places
    private static Metadata amountMetadata() {
        final Map<String, Object> amountAttrs = new HashMap<>(2);
        AttributeConstants.setDisplayPrecision(amountAttrs, (byte) 0);
        return Metadata.metadata(amountAttrs);
    }

    // formatting:off
    interface DataModel {
        int getKey(); // table key

        String getProduct();    DataModel setProduct(String value);

        @ValueMetadata(contentType = AttributeConstants.ContentTypes.Date)
        int getDate();          DataModel setDate(int value);

        @ValueMetadata(contentType = AttributeConstants.ContentTypes.Quantity)
        @DisplayMetadata(precision = 4) // e.g. #,###.0000
        double getAmount();     DataModel setAmount(double value);

        // Values are stored internally as shorts. This reduces object overhead and
        // memory consumption and reduces cycles in value filtering and aggregation.
        // UIs, however, receive this
        @ValueMetadata(contentType = AttributeConstants.ContentTypes.Text)
        short getCountryCode(); DataModel setCountryCode(short value);

        // Values are stored internally as longs. This reduces object overhead and
        // memory consumption and reduces cycles in value filtering and aggregation.
        @ValueMetadata(contentType = AttributeConstants.ContentTypes.Text)
        long getDataCenter(); DataModel setDataCenter(long value);

        @ValueMetadata(contentType = AttributeConstants.ContentTypes.Timestamp,
                       precision   = AttributeConstants.Precisions.Timestamp.Nano)
        @DisplayMetadata(zoneId    =  "America/New_York",
                         precision = AttributeConstants.Precisions.Timestamp.Micro)
        long getTimestamp();    DataModel setTimestamp(long value);
    }
    // formatting:on
}
