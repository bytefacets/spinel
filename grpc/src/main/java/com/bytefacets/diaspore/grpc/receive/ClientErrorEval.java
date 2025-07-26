package com.bytefacets.diaspore.grpc.receive;

import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

import com.bytefacets.diaspore.comms.ConnectionInfo;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;

final class ClientErrorEval {
    private final Logger log;
    private final ConnectionInfo connectionInfo;

    ClientErrorEval(final Logger log, final ConnectionInfo connectionInfo) {
        this.log = requireNonNull(log, "log");
        this.connectionInfo = requireNonNull(connectionInfo, "connectionInfo");
    }

    void handleException(final Throwable t) {
        if (t instanceof StatusRuntimeException statusEx) {
            if (statusEx.getStatus().getCode().equals(Status.Code.UNAVAILABLE)) {
                handleUnavailable(statusEx);
                return;
            }
        }
        log.warn("Error on {}", connectionInfo, t);
    }

    private void handleUnavailable(final StatusRuntimeException statusEx) {
        final String message = requireNonNullElse(statusEx.getCause(), statusEx).getMessage();
        log.warn("Connection unavailable: {} {}", connectionInfo, message);
    }
}
