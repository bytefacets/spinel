// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.filter;

import static com.bytefacets.diaspore.common.jexl.JexlRowContext.jexlRowContext;
import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.common.jexl.JexlEngineProvider;
import com.bytefacets.diaspore.common.jexl.JexlRowContext;
import com.bytefacets.diaspore.schema.FieldResolver;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlScript;

/**
 * A RowPredicate implementation that uses a jexl expression for evaluation over each row.
 * Expressions should evaluate to a Boolean, or the test method will return false.
 *
 * <p>Note that because of how jexl evaluates, FieldNotFound is not thrown immediately upon binding
 * to the schema, but will fire if the rowContext does not have the field mapped.
 *
 * <p>The field mapping done by {@link JexlRowContext} uses `unboundParameters` and `variableList`
 * from the JexlScript and between the two calls, it's expected that all the possible referenced
 * field names are found. This mapping needs to happen during binding so that the operator knows
 * which fields can cause the predicate result to change for the row. In other words, if the
 * JexlScript doesn't report an unbound parameter or variable of "quantity", then the predicate will
 * not report to the Filter operator that a change in "quantity" will affect the outcome of the
 * predicate.
 *
 * <p>Examples of expressions (where they evaluate to a boolean result
 *
 * <ul>
 *   <li>quantity * price >= 10000
 *   <li>side == \"buy\"
 *   <li>side == 1
 *   <li>account != null && account.toLowerCase() == \"client5\"
 *   <li>symbol.startsWith(\"GO\")
 * </ul>
 *
 * @see JexlRowContext
 * @see <a href="https://commons.apache.org/proper/commons-jexl/reference/syntax.html">Jexl
 *     Syntax</a>
 */
public final class JexlRowPredicate implements RowPredicate {
    // VisibleForTesting
    static final JexlEngine DEFAULT_ENGINE = JexlEngineProvider.defaultJexlEngine();
    private final JexlRowContext rowContext;
    private final JexlScript script;

    public static JexlRowPredicate jexlPredicate(final String expression) {
        return jexlPredicate(DEFAULT_ENGINE, expression);
    }

    public static JexlRowPredicate jexlPredicate(
            final JexlEngine jexlEngine, final String expression) {
        return new JexlRowPredicate(jexlEngine.createScript(expression));
    }

    JexlRowPredicate(final JexlScript script) {
        this(script, jexlRowContext(script));
    }

    JexlRowPredicate(final JexlScript script, final JexlRowContext rowContext) {
        this.script = requireNonNull(script, "script");
        this.rowContext = requireNonNull(rowContext, "rowContext");
    }

    public String source() {
        return rowContext.source();
    }

    /**
     * Evaluate the expression over the given row, and return the boolean result or false if the
     * result is not a boolean.
     */
    @Override
    public boolean testRow(final int row) {
        rowContext.setCurrentRow(row);
        final Object result = script.execute(rowContext);
        return result instanceof Boolean ? ((Boolean) result) : false;
    }

    @Override
    public void bindToSchema(final FieldResolver fieldResolver) {
        rowContext.bindToSchema(fieldResolver);
        rowContext.setCurrentRow(-1);
    }

    @Override
    public void unbindSchema() {
        rowContext.unbindSchema();
        rowContext.setCurrentRow(-1);
    }
}
