package com.bytefacets.diaspore.grpc.receive;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;

final class ClientErrorEval {
    enum EvalResponse {
        Retriable,
        NotRetriable
    }

    private final Logger log;
    private final String logPrefix;

    ClientErrorEval(final Logger log, final String logPrefix) {
        this.log = requireNonNull(log, "log");
        this.logPrefix = requireNonNull(logPrefix, "logPrefix");
    }

    EvalResponse handleException(final Throwable t) {
        if (t instanceof StatusRuntimeException statusEx) {
            if (statusEx.getStatus().getCode().equals(Status.Code.UNAVAILABLE)) {
                handleUnavailable(statusEx);
                return EvalResponse.Retriable;
            } else if (statusEx.getStatus().getCode().equals(Status.Code.UNAUTHENTICATED)) {
                handleUnauthenticated(statusEx);
                return EvalResponse.NotRetriable;
            }
        }
        log.warn("{} Error: ", logPrefix, t);
        return EvalResponse.Retriable;
    }

    private void handleUnavailable(final StatusRuntimeException statusEx) {
        final String message = requireNonNullElse(statusEx.getCause(), statusEx).getMessage();
        log.warn("{} Connection unavailable: {}", logPrefix, message);
    }

    private void handleUnauthenticated(final StatusRuntimeException statusEx) {
        final String message = requireNonNullElse(statusEx.getCause(), statusEx).getMessage();
        log.warn("{} Unauthenticated: {}", logPrefix, message);
    }
}
