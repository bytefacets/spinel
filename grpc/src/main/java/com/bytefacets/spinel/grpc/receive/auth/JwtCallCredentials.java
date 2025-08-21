// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.receive.auth;

import static java.util.Objects.requireNonNull;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * Credentials for adding to a service stub. For the GrpcClient, you can use a specializer in the
 * GrpcClientBuilder, for example,
 *
 * <pre>
 * // note the event loop should be single threaded
 * EventLoop dataEventLoop = new DefaultEventLoop();
 * JwtCallCredentials creds = jwtCredentials("bob", "bob-user", "pizza");
 * GrpcClient client = GrpcClientBuilder.grpcClient(channel, dataEventLoop)
 *                       .connectionInfo(new ConnectionInfo("some-server", "0.0.0.0:15000"))
 *                       .withSpecializer(stub -> stub.withCallCredentials(creds))
 *                       .build();
 * </pre>
 *
 * Note that the {@link com.bytefacets.spinel.grpc.send.auth.MultiTenantJwtInterceptor
 * MultiTenantJwtInterceptor} authenticates the issuer/secret pair, and uses the subject as the
 * user.
 *
 * @see com.bytefacets.spinel.grpc.proto.DataServiceGrpc.DataServiceStub
 * @see
 *     com.bytefacets.spinel.grpc.receive.GrpcClientBuilder#withSpecializer(java.util.function.Function)
 * @see com.bytefacets.spinel.grpc.send.auth.MultiTenantJwtInterceptor MultiTenantJwtInterceptor for
 *     the server side of this
 */
public final class JwtCallCredentials extends CallCredentials {
    static final Metadata.Key<String> AUTH_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    private final Supplier<String> tokenGenerator;

    /**
     * Note that the {@link com.bytefacets.spinel.grpc.send.auth.MultiTenantJwtInterceptor
     * MultiTenantJwtInterceptor} authenticates the issuer/secret pair, and uses the subject as the
     * user.
     */
    public static JwtCallCredentials jwtCredentials(
            final String issuer, final String subject, final String secret) {
        return new JwtCallCredentials(() -> generateToken(issuer, subject, secret));
    }

    public static JwtCallCredentials jwtCredentials(final Supplier<String> tokenGenerator) {
        return new JwtCallCredentials(tokenGenerator);
    }

    JwtCallCredentials(final Supplier<String> tokenGenerator) {
        this.tokenGenerator = requireNonNull(tokenGenerator, "tokenGenerator");
    }

    @Override
    public void applyRequestMetadata(
            final RequestInfo requestInfo, final Executor executor, final MetadataApplier applier) {
        executor.execute(() -> apply(applier));
    }

    void apply(final MetadataApplier applier) {
        try {
            final Metadata headers = new Metadata();
            headers.put(AUTH_KEY, "Bearer " + tokenGenerator.get());
            applier.apply(headers);
        } catch (Exception e) {
            applier.fail(Status.UNAUTHENTICATED.withCause(e));
        }
    }

    private static String generateToken(
            final String issuer, final String subject, final String secret) {
        return JWT.create().withIssuer(issuer).withSubject(subject).sign(Algorithm.HMAC256(secret));
    }
}
