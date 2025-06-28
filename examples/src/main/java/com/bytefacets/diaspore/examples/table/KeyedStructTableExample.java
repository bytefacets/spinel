package com.bytefacets.diaspore.examples.table;

import static com.bytefacets.diaspore.table.IntIndexedStructTableBuilder.intIndexedStructTable;
import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.printer.OutputPrinter;
import com.bytefacets.diaspore.table.IntIndexedStructTable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * This example demonstrates using an IntIndexedTable to store incoming data, in this example, some
 * kind of "order" that has a quantity, price, and name.
 */
public final class KeyedStructTableExample {
    // how many orders we're going to produce in each cycle
    private static final int BATCH_SIZE = 100;

    private KeyedStructTableExample() {}

    @SuppressWarnings("resource")
    public static void main(final String[] args) throws InterruptedException {
        final IntIndexedStructTable<ReceivedOrder> table = createTable();
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
        // REVISIT: jdk-21 auto-close
        final var exec = Executors.newSingleThreadScheduledExecutor();
        exec.scheduleAtFixedRate(producer::produce, 1000, 1000, TimeUnit.MILLISECONDS);
        exitLatch.await(); // and wait to exit the example
        exec.shutdownNow();
    }

    private static final class TableWriter {
        private final IntIndexedStructTable<ReceivedOrder> table;
        private final ReceivedOrder facade;

        private TableWriter(final IntIndexedStructTable<ReceivedOrder> table) {
            this.table = requireNonNull(table, "table");
            this.facade = table.createFacade();
        }

        private void onOrder(
                final int orderId,
                final char buySell,
                final int quantity,
                final BigDecimal price,
                final String productName) {
            table.beginUpsert(orderId, facade)
                    .setQuantity(quantity)
                    .setBuySell(buySell)
                    .setPrice(price)
                    .setProductName(productName);
            table.endUpsert();
        }

        private void fire() {
            table.fireChanges();
        }
    }

    private static IntIndexedStructTable<ReceivedOrder> createTable() {
        return intIndexedStructTable(ReceivedOrder.class).initialSize(1000).chunkSize(256).build();
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
            final char buySell = order % 3 == 0 ? 'B' : 'S';
            final BigDecimal price =
                    BigDecimal.valueOf(5 + ((double) order / 100d))
                            .setScale(2, RoundingMode.HALF_UP);
            final String productName = productNames.get(order % productNames.size());
            writer.onOrder(order, buySell, quantity, price, productName);
        }
    }

    interface ReceivedOrder {
        int getOrderId(); // only getter, because it's the key

        // getters only necessary if you're going to read from the table with this interface
        int getQuantity();

        char getBuySell();

        BigDecimal getPrice();

        String getProductName();

        // setters will be translated into fields in the table schema
        ReceivedOrder setQuantity(int v);

        ReceivedOrder setBuySell(char v);

        ReceivedOrder setPrice(BigDecimal v);

        ReceivedOrder setProductName(String v);
    }
}
