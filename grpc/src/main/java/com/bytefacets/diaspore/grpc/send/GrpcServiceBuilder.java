package com.bytefacets.diaspore.grpc.send;

import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.comms.send.OutputRegistry;
import java.util.concurrent.Executor;

public final class GrpcServiceBuilder {
    private final OutputRegistry outputRegistry;
    private final Executor dataExecutor;

    private GrpcServiceBuilder(final OutputRegistry outputRegistry, final Executor dataExecutor) {
        this.outputRegistry = requireNonNull(outputRegistry, "outputRegistry");
        this.dataExecutor = requireNonNull(dataExecutor, "dataExecutor");
    }

    public static GrpcServiceBuilder grpcService(
            final OutputRegistry outputRegistry, final Executor dataExecutor) {
        return new GrpcServiceBuilder(outputRegistry, dataExecutor);
    }

    public GrpcService build() {
        return new GrpcService(outputRegistry, dataExecutor);
    }
}
