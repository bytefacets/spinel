// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.printer;

import static java.util.Objects.requireNonNull;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.text.DecimalFormat;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParsePosition;

/** Renders thousands, millions, billions, trillions */
final class KmbtRenderState {
    private double num;
    private String unit;
    private DecimalFormat format;

    @SuppressFBWarnings(
            value = "SE_BAD_FIELD",
            justification = "complains that KmbtRenderState is not Serializable")
    static NumberFormat kmbFormat(final DecimalFormat format) {
        final KmbtRenderState state = new KmbtRenderState(format);
        return new NumberFormat() {
            @Override
            public StringBuffer format(
                    final double number, final StringBuffer toAppendTo, final FieldPosition pos) {
                return state.render(number, toAppendTo);
            }

            @Override
            public StringBuffer format(
                    final long number, final StringBuffer toAppendTo, final FieldPosition pos) {
                return state.render(number, toAppendTo);
            }

            @Override
            public Number parse(final String source, final ParsePosition parsePosition) {
                throw new UnsupportedOperationException("");
            }
        };
    }

    KmbtRenderState(final DecimalFormat format) {
        this.format = requireNonNull(format, "format");
    }

    StringBuffer render(final double value, final StringBuffer target) {
        set(value);
        target.append(format.format(num)).append(unit);
        return target;
    }

    private void set(final double value) {
        num = value;
        final double test = Math.abs(num);
        if (test >= 1_000_000_000_000d) {
            num /= 1_000_000_000_000d;
            unit = "T";
        } else if (test >= 1_000_000_000d) {
            num /= 1_000_000_000d;
            unit = "B";
        } else if (test >= 1_000_000) {
            num /= 1_000_000d;
            unit = "M";
        } else if (test >= 1_000) {
            num /= 1_000d;
            unit = "k";
        } else {
            unit = "";
        }
    }
}
