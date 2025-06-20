package com.bytefacets.diaspore.examples.table;

import static com.bytefacets.diaspore.schema.FieldDescriptor.doubleField;
import static com.bytefacets.diaspore.schema.FieldDescriptor.intField;
import static com.bytefacets.diaspore.schema.FieldDescriptor.stringField;
import static com.bytefacets.diaspore.table.IntIndexedTableBuilder.intIndexedTable;
import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.printer.OutputPrinter;
import com.bytefacets.diaspore.schema.DoubleWritableField;
import com.bytefacets.diaspore.schema.IntWritableField;
import com.bytefacets.diaspore.schema.StringWritableField;
import com.bytefacets.diaspore.table.IntIndexedTable;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * This example demonstrates using an IntIndexedTable to store incoming data, in this example, some
 * kind of "order" that has a quantity, price, and name.
 */
public final class KeyedTableExample {
    // how many orders we're going to produce in each cycle
    private static final int BATCH_SIZE = 100;

    private KeyedTableExample() {}

    public static void main(final String[] args) throws InterruptedException {
        final IntIndexedTable table = createTable();
        // we're connecting a simple printer here so we can see what's happening
        // but in a real process, we would attach other, more useful things
        table.output().attachInput(OutputPrinter.printer().input());

        // create a little class which helps write into the table's fields
        final TableWriter writer = new TableWriter(table);

        // these are just supporting our little example, to produce some number of
        // order batches
        final CountDownLatch exitLatch = new CountDownLatch(100);
        final MockOrderProducer producer = new MockOrderProducer(writer, exitLatch);

        // schedule batches to "arrive" every 1 second
        try (var exec = Executors.newSingleThreadScheduledExecutor()) {
            exec.scheduleAtFixedRate(producer::produce, 1000, 1000, TimeUnit.MILLISECONDS);
            exitLatch.await(); // and wait to exit the example
        }
    }

    private static final class TableWriter {
        private final IntIndexedTable table;
        private final IntWritableField quantityField;
        private final DoubleWritableField priceField;
        private final StringWritableField productNameField;

        private TableWriter(final IntIndexedTable table) {
            this.table = requireNonNull(table, "table");
            quantityField = table.writableField("quantity");
            priceField = table.writableField("price");
            productNameField = table.writableField("product_name");
        }

        private void onOrder(
                final int orderId,
                final int quantity,
                final double price,
                final String productName) {
            final int row = table.beginUpsert(orderId);
            quantityField.setValueAt(row, quantity);
            priceField.setValueAt(row, price);
            productNameField.setValueAt(row, productName);
            table.endUpsert();
        }

        private void fire() {
            table.fireChanges();
        }
    }

    private static IntIndexedTable createTable() {
        return intIndexedTable("orders")
                .initialSize(1000)
                .keyFieldName("order_id")
                .chunkSize(256)
                .addFields(intField("quantity"), doubleField("price"), stringField("product_name"))
                .build();
    }

    private static class MockOrderProducer {
        private final TableWriter writer;
        private final CountDownLatch exitLatch;
        private int nextOrderId = 700;
        private final List<String> productNames =
                IntStream.range(0, 10).mapToObj(i -> "product-" + i).toList();

        MockOrderProducer(final TableWriter writer, final CountDownLatch exitLatch) {
            this.writer = writer;
            this.exitLatch = exitLatch;
        }

        private void produce() {
            for (int i = 0; i < BATCH_SIZE; i++) {
                produceOneMockOrder();
            }
            writer.fire();
            exitLatch.countDown();
        }

        private void produceOneMockOrder() {
            final int order = nextOrderId++;
            final int quantity = 10 * (order % 7);
            final double price = 5 + ((double) order / 100);
            final String productName = productNames.get(order % productNames.size());
            writer.onOrder(order, quantity, price, productName);
        }
    }
}
