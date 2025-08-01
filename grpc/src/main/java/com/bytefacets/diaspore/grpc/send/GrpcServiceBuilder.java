package com.bytefacets.diaspore.grpc.send;

import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.comms.send.OutputRegistry;
import io.netty.channel.EventLoop;

public final class GrpcServiceBuilder {
    private final OutputRegistry outputRegistry;
    private final EventLoop dataEventLoop;

    private GrpcServiceBuilder(final OutputRegistry outputRegistry, final EventLoop dataEventLoop) {
        this.outputRegistry = requireNonNull(outputRegistry, "outputRegistry");
        this.dataEventLoop = requireNonNull(dataEventLoop, "dataEventLoop");
    }

    public static GrpcServiceBuilder grpcService(
            final OutputRegistry outputRegistry, final EventLoop dataEventLoop) {
        return new GrpcServiceBuilder(outputRegistry, dataEventLoop);
    }

    public GrpcService build() {
        return new GrpcService(outputRegistry, dataEventLoop);
    }
}
