// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.send.auth;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.bytefacets.spinel.comms.send.ConnectedSessionInfo;
import com.bytefacets.spinel.grpc.send.GrpcService;
import io.grpc.Attributes;
import io.grpc.Context;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MultiTenantJwtInterceptorTest {
    private @Mock ServerCall<String, String> call;
    private @Mock ServerCallHandler<String, String> handler;
    private @Mock Function<String, String> tokenLookup;
    private @Captor ArgumentCaptor<Status> statusCaptor;
    private final Metadata metadata = new Metadata();
    private MultiTenantJwtInterceptor interceptor;

    @BeforeEach
    void setUp() {
        interceptor = new MultiTenantJwtInterceptor(tokenLookup);
    }

    @Test
    void shouldCloseWithUnauthenticatedWhenHeaderMissing() {
        interceptor.interceptCall(call, metadata, handler);
        verify(call, times(1)).close(statusCaptor.capture(), any(Metadata.class));
        assertThat(statusCaptor.getValue().getCode(), equalTo(Status.UNAUTHENTICATED.getCode()));
        assertThat(
                statusCaptor.getValue().getDescription(),
                equalTo("Missing token in authorization header"));
    }

    @Test
    void shouldCloseWithUnauthenticatedWhenHeaderNotBearer() {
        metadata.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "foo");
        interceptor.interceptCall(call, metadata, handler);
        verify(call, times(1)).close(statusCaptor.capture(), any(Metadata.class));
        assertThat(statusCaptor.getValue().getCode(), equalTo(Status.UNAUTHENTICATED.getCode()));
        assertThat(
                statusCaptor.getValue().getDescription(),
                equalTo("Missing token in authorization header"));
    }

    @Test
    void shouldCloseWithUnauthenticatedWhenUnknownTenant() {
        metadata.put(
                Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
                "Bearer " + generateToken());
        interceptor.interceptCall(call, metadata, handler);
        verify(call, times(1)).close(statusCaptor.capture(), any(Metadata.class));
        assertThat(statusCaptor.getValue().getCode(), equalTo(Status.UNAUTHENTICATED.getCode()));
        assertThat(statusCaptor.getValue().getDescription(), equalTo("Unknown tenant Globo-Chem"));
    }

    @Test
    void shouldCloseWithUnauthenticatedWhenSecretLookupException() {
        doThrow(new RuntimeException("LOOKUP FAILURE!!")).when(tokenLookup).apply(any());
        metadata.put(
                Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
                "Bearer " + generateToken());
        interceptor.interceptCall(call, metadata, handler);
        verify(call, times(1)).close(statusCaptor.capture(), any(Metadata.class));
        assertThat(statusCaptor.getValue().getCode(), equalTo(Status.UNAUTHENTICATED.getCode()));
        assertThat(
                statusCaptor.getValue().getDescription(),
                equalTo("Exception looking up tenant secret: LOOKUP FAILURE!!"));
    }

    @Test
    void shouldAddAuthenticatedInfoToContext() {
        setupAuthPass();
        final Attributes mockAttr = mock(Attributes.class);
        when(call.getAttributes()).thenReturn(mockAttr);
        when(mockAttr.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR))
                .thenReturn(new InetSocketAddress("127.0.0.1", 5000));
        // context has to be captured during the call
        final AtomicReference<ConnectedSessionInfo> capturedInfo = new AtomicReference<>();
        doAnswer(
                        inv -> {
                            capturedInfo.set(
                                    GrpcService.CONNECTED_SESSION_KEY.get(Context.current()));
                            return null;
                        })
                .when(handler)
                .startCall(any(), any());

        // when
        interceptor.interceptCall(call, metadata, handler);

        // then
        assertThat(capturedInfo.get().getTenant(), equalTo("Globo-Chem"));
        assertThat(capturedInfo.get().getUser(), equalTo("Pit Pat"));
        assertThat(capturedInfo.get().getRemote(), equalTo("/127.0.0.1:5000"));
    }

    @Test
    void shouldNotCloseCallWhenAuthenticated() {
        setupAuthPass();
        interceptor.interceptCall(call, metadata, handler);
        verify(call, never()).close(any(), any());
    }

    @Test
    void shouldStartCallWhenAuthenticated() {
        setupAuthPass();
        interceptor.interceptCall(call, metadata, handler);
        verify(handler, times(1)).startCall(call, metadata);
    }

    private void setupAuthPass() {
        when(tokenLookup.apply("Globo-Chem")).thenReturn("bag-hutch");
        metadata.put(
                Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
                "Bearer " + generateToken());
    }

    private static String generateToken() {
        return JWT.create()
                .withIssuer("Globo-Chem")
                .withSubject("Pit Pat")
                .sign(Algorithm.HMAC256("bag-hutch"));
    }
}
