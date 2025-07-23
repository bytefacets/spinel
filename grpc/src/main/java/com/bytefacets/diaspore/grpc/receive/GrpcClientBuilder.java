package com.bytefacets.diaspore.grpc.receive;

import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.comms.ConnectionInfo;
import io.grpc.ManagedChannel;
import java.net.URI;
import java.util.concurrent.Executor;

public final class GrpcClientBuilder {
    private static final ConnectionInfo EMPTY = new ConnectionInfo("", URI.create("grpc://unset"));
    private final ManagedChannel channel;
    private final Executor dataExecutor;
    private ConnectionInfo connectionInfo = EMPTY;

    private GrpcClientBuilder(final ManagedChannel channel, final Executor dataExecutor) {
        this.channel = requireNonNull(channel, "channel");
        this.dataExecutor = requireNonNull(dataExecutor, "dataExecutor");
    }

    public static GrpcClientBuilder grpcClient(
            final ManagedChannel channel, final Executor dataExecutor) {
        return new GrpcClientBuilder(channel, dataExecutor);
    }

    public GrpcClientBuilder connectionInfo(final ConnectionInfo connectionInfo) {
        this.connectionInfo = connectionInfo;
        return this;
    }

    public GrpcClient build() {
        return new GrpcClient(connectionInfo, channel, dataExecutor);
    }
}
