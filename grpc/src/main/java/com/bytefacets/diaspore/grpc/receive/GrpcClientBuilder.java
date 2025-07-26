package com.bytefacets.diaspore.grpc.receive;

import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.comms.ConnectionInfo;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.netty.channel.EventLoop;
import java.net.URI;

public final class GrpcClientBuilder {
    private static final ConnectionInfo EMPTY = new ConnectionInfo("", URI.create("grpc://unset"));
    private final ManagedChannel channel;
    private final EventLoop dataEventLoop;
    private ConnectionInfo connectionInfo = EMPTY;

    private GrpcClientBuilder(final ManagedChannel channel, final EventLoop dataEventLoop) {
        this.channel = requireNonNull(channel, "channel");
        this.dataEventLoop = requireNonNull(dataEventLoop, "dataEventLoop");
    }

    public static GrpcClientBuilder grpcClient(
            final ManagedChannel channel, final EventLoop dataEventLoop) {
        return new GrpcClientBuilder(channel, dataEventLoop);
    }

    public GrpcClientBuilder connectionInfo(final ConnectionInfo connectionInfo) {
        this.connectionInfo = connectionInfo;
        return this;
    }

    public GrpcClient build() {
        return new GrpcClient(connectionInfo, channel, dataEventLoop);
    }
}
