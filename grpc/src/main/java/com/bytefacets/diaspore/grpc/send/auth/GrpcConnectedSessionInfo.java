package com.bytefacets.diaspore.grpc.send.auth;

import com.bytefacets.diaspore.comms.send.ConnectedSessionInfo;

public final class GrpcConnectedSessionInfo implements ConnectedSessionInfo {
    public static final GrpcConnectedSessionInfo EMPTY =
            new GrpcConnectedSessionInfo("anon", "anon", "");
    private final String tenant;
    private final String user;
    private final String remote;

    GrpcConnectedSessionInfo(final String tenant, final String user, final String remote) {
        this.tenant = tenant;
        this.user = user;
        this.remote = remote;
    }

    @Override
    public String getTenant() {
        return tenant;
    }

    @Override
    public String getUser() {
        return user;
    }

    @Override
    public String getRemote() {
        return remote;
    }

    @Override
    public String toString() {
        return String.format("%s/%s@%s", tenant, user, remote);
    }
}
