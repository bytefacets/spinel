package com.bytefacets.diaspore.common;

import com.bytefacets.diaspore.TransformInput;
import com.bytefacets.diaspore.TransformOutput;
import com.bytefacets.diaspore.transform.InputProvider;
import com.bytefacets.diaspore.transform.OutputProvider;

/**
 * Convenience class for connection operations between operators
 */
public final class Connector {
    private Connector() {}

    public static void connectOutputToInput(
            final OutputProvider outputProvider, final InputProvider inputProvider) {
        connectOutputToInput(outputProvider.output(), inputProvider.input());
    }

    public static void connectOutputToInput(
            final OutputProvider outputProvider, final TransformInput input) {
        connectOutputToInput(outputProvider.output(), input);
    }

    public static void connectOutputToInput(
            final TransformOutput output, final InputProvider inputProvider) {
        connectOutputToInput(output, inputProvider.input());
    }

    public static void connectOutputToInput(
            final TransformOutput output, final TransformInput input) {
        output.attachInput(input);
    }

    public static void connectInputToOutput(
            final InputProvider inputProvider, final OutputProvider outputProvider) {
        connectOutputToInput(outputProvider, inputProvider);
    }

    public static void connectInputToOutput(
            final TransformInput input, final OutputProvider outputProvider) {
        connectOutputToInput(outputProvider, input);
    }

    public static void connectInputToOutput(
            final TransformInput input, final TransformOutput output) {
        connectOutputToInput(output, input);
    }

    public static void connectInputToOutput(
            final InputProvider inputProvider, final TransformOutput output) {
        connectOutputToInput(output, inputProvider);
    }

    public static OutputConnector connectOutput(final OutputProvider outputProvider) {
        return new OutputConnector() {
            @Override
            public void toInput(final InputProvider input) {
                connectOutputToInput(outputProvider, input);
            }

            @Override
            public void toInput(final TransformInput input) {
                connectOutputToInput(outputProvider, input);
            }
        };
    }

    public static OutputConnector connectOutput(final TransformOutput output) {
        return connectOutput(() -> output);
    }

    public static InputConnector connectInput(final InputProvider inputProvider) {
        return new InputConnector() {
            @Override
            public void toOutput(final OutputProvider output) {
                connectOutputToInput(output, inputProvider);
            }

            @Override
            public void toOutput(final TransformOutput output) {
                connectOutputToInput(output, inputProvider);
            }
        };
    }

    public static InputConnector connectInput(final TransformInput input) {
        return connectInput(() -> input);
    }

    public interface OutputConnector {
        void toInput(InputProvider input);

        void toInput(TransformInput input);
    }

    public interface InputConnector {
        void toOutput(OutputProvider output);

        void toOutput(TransformOutput output);
    }
}
