// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.send.auth;

import static java.util.Objects.requireNonNull;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.bytefacets.spinel.grpc.send.GrpcService;
import com.google.common.base.Preconditions;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.net.SocketAddress;
import java.util.Map;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enables JWT Authentication on a gRPC server.
 *
 * <p>
 *
 * <pre>
 * final GrpcService service = GrpcServiceBuilder.grpcService(registry, eventLoop).build();
 * final Map<String, String> tenantSecrets = Map.of("bob", "bobs-secret");
 * final MultiTenantJwtInterceptor jwtInterceptor =
 *                      MultiTenantJwtInterceptor.multiTenantJwt(tenantSecrets::get);
 * final Server server = ServerBuilder.forPort(port)
 *                         .addService(
 *                                 ServerInterceptors.intercept(service, jwtInterceptor))
 *                         .executor(topologyBuilder.eventLoop)
 *                         .build();
 * </pre>
 */
public final class MultiTenantJwtInterceptor implements ServerInterceptor {
    private static final Logger log = LoggerFactory.getLogger(MultiTenantJwtInterceptor.class);
    static final Metadata EMPTY_METADATA = new Metadata();
    private static final Metadata.Key<String> AUTH_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    private final Function<String, String> tenantSecretProvider; // tenant ID -> secret

    public static MultiTenantJwtInterceptor multiTenantJwt(
            final Map<String, String> tenantSecrets) {
        Preconditions.checkArgument(!tenantSecrets.isEmpty(), "tenantSecrets cannot be empty");
        final var copy = Map.copyOf(requireNonNull(tenantSecrets, "tenantSecrets"));
        return new MultiTenantJwtInterceptor(copy::get);
    }

    public static MultiTenantJwtInterceptor multiTenantJwt(
            final Function<String, String> tenantSecretProvider) {
        return new MultiTenantJwtInterceptor(tenantSecretProvider);
    }

    MultiTenantJwtInterceptor(final Function<String, String> tenantSecretProvider) {
        this.tenantSecretProvider = requireNonNull(tenantSecretProvider, "tenantSecretProvider");
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            final ServerCall<ReqT, RespT> call,
            final Metadata headers,
            final ServerCallHandler<ReqT, RespT> next) {
        final String token = extractToken(headers);
        if (token == null) {
            return closeUnauthenticated(call, "Missing token in authorization header");
        }
        try {
            // Decode without verification to extract `issuer` (or `kid`)
            final DecodedJWT decoded;
            try {
                decoded = JWT.decode(token);
            } catch (Exception ex) {
                return closeUnauthenticated(call, "Invalid token");
            }

            // validate tenant secret
            final String tenantId = decoded.getIssuer(); // or getKeyId()
            final String secret;
            try {
                secret = tenantSecretProvider.apply(tenantId);
                if (secret == null) {
                    return closeUnauthenticated(call, "Unknown tenant " + tenantId);
                }
            } catch (Exception ex) {
                return closeUnauthenticated(
                        call, "Exception looking up tenant secret: " + ex.getMessage(), ex);
            }

            // verify
            try {
                final Algorithm algorithm = Algorithm.HMAC256(secret);
                JWT.require(algorithm).withIssuer(tenantId).build().verify(decoded);
            } catch (Exception ex) {
                return closeUnauthenticated(call, "Exception verifying tenant secret", ex);
            }

            // put stuff into context and return
            final Context ctx = applyToContext(call, tenantId, decoded.getSubject());
            return Contexts.interceptCall(ctx, call, headers, next);
        } catch (Exception e) {
            return closeUnauthenticated(call, "Exception occurred", e);
        }
    }

    private static String extractToken(final Metadata headers) {
        final String authHeader = headers.get(AUTH_KEY);
        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            return null;
        }
        return authHeader.substring(7); // strips "Bearer "
    }

    private static Context applyToContext(
            final ServerCall<?, ?> call, final String tenantId, final String subject) {
        log.info("Authenticated {}/{}", tenantId, subject);
        final var sessionInfo = new GrpcConnectedSessionInfo(tenantId, subject, tryRemote(call));
        return Context.current().withValue(GrpcService.CONNECTED_SESSION_KEY, sessionInfo);
    }

    private static <ReqT, RespT> ServerCall.Listener<ReqT> closeUnauthenticated(
            final ServerCall<ReqT, RespT> call, final String description) {
        call.close(Status.UNAUTHENTICATED.withDescription(description), EMPTY_METADATA);
        return new ServerCall.Listener<>() {};
    }

    private static <ReqT, RespT> ServerCall.Listener<ReqT> closeUnauthenticated(
            final ServerCall<ReqT, RespT> call,
            final String description,
            final Throwable exception) {
        call.close(
                Status.UNAUTHENTICATED.withDescription(description).withCause(exception),
                EMPTY_METADATA);
        return new ServerCall.Listener<>() {};
    }

    private static String tryRemote(final ServerCall<?, ?> call) {
        try {
            final SocketAddress remoteAddr =
                    call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
            return remoteAddr != null ? remoteAddr.toString() : null;
        } catch (Exception ex) {
            log.debug("Could not extract socket address: {}", ex.getMessage());
            return null;
        }
    }
}
