package com.bytefacets.diaspore.grpc.send;

import static java.util.Objects.requireNonNull;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;

final class SenderErrorEval {
    private final Logger log;

    SenderErrorEval(final Logger log) {
        this.log = requireNonNull(log, "log");
    }

    void handleException(final Throwable t) {
        if (t instanceof StatusRuntimeException statusEx) {
            if (statusEx.getStatus().getCode().equals(Status.Code.CANCELLED)) {
                handleCancelled(statusEx);
                return;
            }
        }
        log.warn("Error", t);
    }

    private void handleCancelled(final StatusRuntimeException statusEx) {
        log.warn("Connection dropped by client: {}", statusEx.getMessage());
    }
}
