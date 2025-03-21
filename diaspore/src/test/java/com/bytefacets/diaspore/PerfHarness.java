package com.bytefacets.diaspore;

import static com.bytefacets.diaspore.transform.TransformBuilder.transform;

import com.bytefacets.collections.functional.IntIterable;
import com.bytefacets.diaspore.schema.ChangedFieldSet;
import com.bytefacets.diaspore.schema.Schema;
import com.bytefacets.diaspore.transform.TransformBuilder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import javax.annotation.Nullable;

public final class PerfHarness {
    private final int warmUp;
    private final int run;

    public PerfHarness(final int warmUp, final int run) {
        this.warmUp = warmUp;
        this.run = run;
    }

    public void run(final Callback callback) {
        final TransformBuilder builder = transform();
        final Collector collector = new Collector();
        callback.init(builder, collector);
        doWarmup(callback);
        collector.running = true;
        doRun(callback);
        collector.print();
    }

    public interface Callback {
        void init(TransformBuilder builder, TransformInput collector);

        void warmup(int iteration);

        void step(int iteration);
    }

    @SuppressFBWarnings("DM_GC")
    private void doWarmup(final Callback callback) {
        for (int i = 0; i < warmUp; i++) {
            callback.warmup(i);
        }
        for (int i = 0; i < 10; i++) {
            System.gc();
        }
    }

    private void doRun(final Callback callback) {
        for (int i = 0; i < run; i++) {
            callback.step(i);
        }
    }

    private static class Collector implements TransformInput {
        private final Counter adds = new Counter("adds");
        private final Counter chgs = new Counter("chgs");
        private final Counter rems = new Counter("rems");
        private boolean running = false;

        private void print() {
            System.out.println(adds);
            System.out.println(chgs);
            System.out.println(rems);
        }

        @Override
        public void schemaUpdated(@Nullable final Schema schema) {}

        @Override
        public void rowsAdded(final IntIterable rows) {
            if (running) {
                adds.process(rows);
            }
        }

        @Override
        public void rowsChanged(final IntIterable rows, final ChangedFieldSet changedFields) {
            if (running) {
                chgs.process(rows);
            }
        }

        @Override
        public void rowsRemoved(final IntIterable rows) {
            if (running) {
                rems.process(rows);
            }
        }
    }

    static final class Counter {
        private final String name;
        private int count;
        private int rows;
        private long time;
        private long start;

        private Counter(final String name) {
            this.name = name;
        }

        void process(final IntIterable rows) {
            count++;
            start = System.nanoTime();
            rows.forEach(this::rowCount);
            time += (System.nanoTime() - start);
        }

        private void rowCount(final int i) {
            this.rows++;
        }

        public String toString() {
            return String.format(
                    "%s: %d times %d rows in %s",
                    name, count, rows, Duration.ofNanos(time).truncatedTo(ChronoUnit.MICROS));
        }
    }
}
