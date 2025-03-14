package com.bytefacets.diaspore.gen;

public final class CodeGenException extends RuntimeException {
    private CodeGenException(final String message) {
        super(message);
    }

    private CodeGenException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public static CodeGenException codeGenException(
            final Class<?> type, final String context, final Exception cause) {
        return new CodeGenException(
                String.format(
                        "Exception processing %s during %s: %s",
                        type.getName(), context, cause.getMessage()),
                cause);
    }

    public static CodeGenException codeGenException(
            final Class<?> type, final String context, final String source, final Exception cause) {
        return new CodeGenException(
                String.format(
                        "Exception processing %s during %s with source code: %s",
                        type.getName(), context, source),
                cause);
    }

    public static CodeGenException invalidUserType(final Class<?> type, final String detail) {
        return new CodeGenException(
                String.format("Invalid user type %s: %s", type.getName(), detail));
    }
}
