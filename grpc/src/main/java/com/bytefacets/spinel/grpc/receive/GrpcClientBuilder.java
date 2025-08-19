// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.receive;

import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.comms.ConnectionInfo;
import com.bytefacets.spinel.grpc.proto.DataServiceGrpc;
import io.grpc.ManagedChannel;
import io.netty.channel.EventLoop;
import java.util.function.Function;

public final class GrpcClientBuilder {
    private static final ConnectionInfo EMPTY = new ConnectionInfo("", "");
    private final ManagedChannel channel;
    private final EventLoop dataEventLoop;
    private ConnectionInfo connectionInfo = EMPTY;
    private Function<DataServiceGrpc.DataServiceStub, DataServiceGrpc.DataServiceStub>
            stubSpecializer = stub -> stub;

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

    public GrpcClientBuilder withSpecializer(
            final Function<DataServiceGrpc.DataServiceStub, DataServiceGrpc.DataServiceStub>
                    stubSpecializer) {
        this.stubSpecializer = requireNonNull(stubSpecializer, "stubSpecializer");
        return this;
    }

    DataServiceGrpc.DataServiceStub createStub() {
        return stubSpecializer.apply(
                DataServiceGrpc.newStub(channel).withExecutor(dataEventLoop).withWaitForReady());
    }

    public GrpcClient build() {
        return new GrpcClient(connectionInfo, channel, createStub(), dataEventLoop);
    }
}
