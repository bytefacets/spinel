package com.bytefacets.diaspore.transform;

public final class TransformException extends RuntimeException {
    public TransformException(final String message) {
        super(message);
    }

    static TransformException notFound(final String name) {
        return new TransformException("Operator not found: " + name);
    }

    static TransformException notAnOutputProvider(final String name, final Object operator) {
        return new TransformException(
                String.format(
                        "Requested operator is not an OutputProvider: %s is %s",
                        name, operator.getClass().getName()));
    }
}
