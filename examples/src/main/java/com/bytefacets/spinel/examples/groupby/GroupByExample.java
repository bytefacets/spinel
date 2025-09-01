// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.examples.groupby;

import static com.bytefacets.spinel.groupby.lib.SumFactory.sumToDouble;
import static com.bytefacets.spinel.groupby.lib.SumFactory.sumToInt;

import com.bytefacets.collections.queue.IntDeque;
import com.bytefacets.spinel.common.Connector;
import com.bytefacets.spinel.printer.OutputLoggerBuilder;
import com.bytefacets.spinel.projection.lib.DoubleFieldCalculation;
import com.bytefacets.spinel.schema.DoubleField;
import com.bytefacets.spinel.schema.FieldResolver;
import com.bytefacets.spinel.schema.IntField;
import com.bytefacets.spinel.table.IntIndexedStructTable;
import com.bytefacets.spinel.transform.OutputProvider;
import com.bytefacets.spinel.transform.TransformBuilder;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.event.Level;

/**
 * In this example, we take an orders table, add a projection to calculate the notional (i.e. the
 * amount), then aggregate by instrument, so that we have a result that's the instrument, count of
 * orders, total quantity, and total amount. After the GroupBy, we calculate an average price over
 * the group by dividing the total amount by the total quantity.
 *
 * <p>You'll see updates that modify the aggregation and projections, as well.
 *
 * <pre>
 * orders -> projection (notional calculation) -> group by(instrument) -> projection (avg price)
 * </pre>
 */
final class GroupByExample {
    private static final int ORDER_LIMIT = 8;
    private static final Random random = new Random(26752098);

    private GroupByExample() {}

    @SuppressWarnings("resource")
    public static void main(final String[] args) throws Exception {
        final TransformBuilder transform = TransformBuilder.transform();
        // formatting:off
        transform.intIndexedStructTable(Order.class)
             .then()
                .project("EnrichedOrder")
                   // define a new field, calculated by NotionalCalculation
                   .lazyCalculation("Notional", new NotionalCalculation())
             .then()
                .groupBy("ByInstrument")
                   // define the aggregation over the int field "InstrumentId"
                   .groupByFields("InstrumentId")
                   // a Count field is a special case aggregation bc we are already tracking this
                   .includeCountField("Count")
                   // helpful for parent-child views
                   .includeGroupIdField("GroupId")
                   // sum the Qty field and call it TotalQty
                   .addAggregation(sumToInt("Qty", "TotalQty"))
                   // sum the Notional field and call it TotalNotional
                   .addAggregation(sumToDouble("Notional", "TotalNotional"))
             .thenWithParentOutput()
                // another projection for a calculation on the aggregation output
                .project("AvgPriceProjection")
                   // a new Double AvgPrice field implemented by AvgPriceCalculation
                   .lazyCalculation("AvgPrice", new AvgPriceCalculation());
        transform.build();
        // log to demonstrate
        Connector.connectInputToOutput(
                OutputLoggerBuilder.logger("EnrichedOrderLog").logLevel(Level.INFO).build(),
                (OutputProvider)transform.lookupNode("EnrichedOrder"));
        Connector.connectInputToOutput(
                OutputLoggerBuilder.logger("  AggregationLog").logLevel(Level.INFO).build(),
                (OutputProvider)transform.lookupNode("AvgPriceProjection"));
        // formatting:on

        final IntIndexedStructTable<Order> orders = transform.lookupNode("Order");
        Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(new Updater(orders), 1, 2, TimeUnit.SECONDS);
        Thread.currentThread().join();
    }

    /** Defines a calculation of a Double field, based on Qty and Price fields. */
    private static final class NotionalCalculation implements DoubleFieldCalculation {
        // Mutable because we don't have these references when the calculation is declared.
        // Also, depending on other circumstances, like receiving a source table from another
        // process, the schema could be re-stated, and we'll need to release our references
        private IntField quantityField;
        private DoubleField priceField;

        // When the schema arrives in the GroupBy operator, the GroupBy will call into this
        // method to bind itself to the inbound schema by requesting fields from the
        // fieldResolver. This is where you acquire the references to the inbound data.
        // The fact that you search for the field in the fieldResolver is recorded so that
        // the GroupBy will know if it sees a change to either of these fields, it needs to
        // call here again. It will also add the referenced fields to the output schema.
        @Override
        public void bindToSchema(final FieldResolver fieldResolver) {
            quantityField = fieldResolver.findIntField("Qty"); // int or castable to int
            priceField = fieldResolver.findDoubleField("Price"); // double or castable to double
        }

        // Calculates the value of this field. The method is given the row as context, and
        // we access our calculation's inputs by indexing into the bound fields
        @Override
        public double calculate(final int row) {
            return quantityField.valueAt(row) * priceField.valueAt(row);
        }

        // In the event a schema is reset, we release our references to the fields.
        @Override
        public void unbindSchema() {
            quantityField = null;
            priceField = null;
        }
    }

    private static final class AvgPriceCalculation implements DoubleFieldCalculation {
        private IntField totalQuantityField;
        private DoubleField totalNotionalField;

        @Override
        public double calculate(final int row) {
            final double notional = totalNotionalField.valueAt(row);
            final int quantity = totalQuantityField.valueAt(row);
            return quantity != 0 ? notional / quantity : 0;
        }

        @Override
        public void bindToSchema(final FieldResolver fieldResolver) {
            totalQuantityField = fieldResolver.findIntField("TotalQty");
            totalNotionalField = fieldResolver.findDoubleField("TotalNotional");
        }

        @Override
        public void unbindSchema() {
            totalQuantityField = null;
            totalNotionalField = null;
        }
    }

    /**
     * Add, Change, and Remove orders. We're only specifying the Account, Instrument, Price, and
     * Quantity. Downstream of the orders table, projections and aggregations will compute other
     * fields.
     */
    private static final class Updater implements Runnable {
        final IntIndexedStructTable<Order> orders;
        private final Order facade;
        private final IntDeque activeOrders = new IntDeque(16);
        private int nextId = 1;

        private Updater(final IntIndexedStructTable<Order> orders) {
            this.orders = orders;
            this.facade = orders.createFacade();
        }

        @Override
        public void run() {
            if (activeOrders.size() == ORDER_LIMIT) {
                final int orderId = activeOrders.removeFirst();
                if (modify(orderId)) {
                    activeOrders.addLast(orderId);
                }
            } else {
                activeOrders.addLast(add());
            }
            orders.fireChanges();
        }

        private int add() {
            final int orderId = nextId++;
            final int instrumentId = (orderId % 5) + 1;
            final double price = (instrumentId * 100) + random.nextInt(30, 80);
            orders.beginAdd(orderId, facade)
                    .setAccount("Account" + (orderId % 3))
                    .setInstrumentId(orderId % 5)
                    .setPrice(price / 100d)
                    .setQty(100 * random.nextInt(1, 7));
            orders.endAdd();
            return orderId;
        }

        private boolean modify(final int orderId) {
            final int row = orders.lookupKeyRow(orderId);
            orders.moveToRow(facade, row);
            final int newQty = facade.getQty() - (random.nextInt(1, 3) * 100);
            if (newQty <= 0) {
                orders.remove(orderId);
                return false;
            } else {
                orders.beginChange(orderId, facade).setQty(newQty);
                orders.endChange();
                return true;
            }
        }
    }

    // formatting:off
    /** A simplified model of an Order which will get inspected at turned into a table structure. */
    public interface Order {
        int getOrderId();      // getter only bc it's the key field
        String getAccount();   Order setAccount(String value);
        int getInstrumentId(); Order setInstrumentId(int value);
        int getQty();          Order setQty(int value);
        double getPrice();     Order setPrice(double value);
    }
    // formatting:on
}
