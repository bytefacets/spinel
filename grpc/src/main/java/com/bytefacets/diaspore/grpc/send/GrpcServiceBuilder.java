package com.bytefacets.diaspore.grpc.send;

import static java.util.Objects.requireNonNull;

import com.bytefacets.diaspore.comms.send.OutputRegistryFactory;
import io.netty.channel.EventLoop;

public final class GrpcServiceBuilder {
    private final OutputRegistryFactory outputRegistryFactory;
    private final EventLoop dataEventLoop;

    private GrpcServiceBuilder(
            final OutputRegistryFactory outputRegistryFactory, final EventLoop dataEventLoop) {
        this.outputRegistryFactory = requireNonNull(outputRegistryFactory, "outputRegistryFactory");
        this.dataEventLoop = requireNonNull(dataEventLoop, "dataEventLoop");
    }

    public static GrpcServiceBuilder grpcService(
            final OutputRegistryFactory outputRegistryFactory, final EventLoop dataEventLoop) {
        return new GrpcServiceBuilder(outputRegistryFactory, dataEventLoop);
    }

    public GrpcService build() {
        return new GrpcService(outputRegistryFactory, dataEventLoop);
    }
}
