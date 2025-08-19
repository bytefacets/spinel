// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.grpc.send;

import static java.util.Objects.requireNonNull;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;

final class SenderErrorEval {
    private final Logger log;
    private final String logPrefix;

    SenderErrorEval(final Logger log, final String logPrefix) {
        this.log = requireNonNull(log, "log");
        this.logPrefix = logPrefix;
    }

    void handleException(final Throwable t) {
        if (t instanceof StatusRuntimeException statusEx) {
            if (statusEx.getStatus().getCode().equals(Status.Code.CANCELLED)) {
                handleCancelled(statusEx);
                return;
            }
        }
        log.warn("{} Error", logPrefix, t);
    }

    private void handleCancelled(final StatusRuntimeException statusEx) {
        log.warn("{} Connection dropped by client: {}", logPrefix, statusEx.getMessage());
    }
}
