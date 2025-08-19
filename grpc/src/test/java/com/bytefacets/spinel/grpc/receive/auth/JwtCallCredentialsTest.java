// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.grpc.receive.auth;

import static com.bytefacets.spinel.grpc.receive.auth.JwtCallCredentials.jwtCredentials;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class JwtCallCredentialsTest {
    private @Mock CallCredentials.RequestInfo requestInfo;
    private @Mock Executor executor;
    private @Mock CallCredentials.MetadataApplier applier;
    private @Captor ArgumentCaptor<Metadata> metadataCaptor;
    private final JwtCallCredentials creds = jwtCredentials("Globo-Chem", "Pit Pat", "bag-hutch");

    @BeforeEach
    void setUp() {
        doAnswer(
                        inv -> {
                            inv.getArgument(0, Runnable.class).run();
                            return null;
                        })
                .when(executor)
                .execute(any());
    }

    @Test
    void shouldApplyAuthenticationHeaderWithExecutor() {
        creds.applyRequestMetadata(requestInfo, executor, applier);
        verify(executor, times(1)).execute(any());
        verify(applier, times(1)).apply(metadataCaptor.capture());
        validateHeader(metadataCaptor.getValue());
    }

    @Test
    void shouldApplyAuthenticationHeader() {
        creds.applyRequestMetadata(requestInfo, executor, applier);
        verify(applier, times(1)).apply(metadataCaptor.capture());
        validateHeader(metadataCaptor.getValue());
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldFailWhenTokenGenThrowsException() {
        final Exception exception = new RuntimeException("FAILED!!");
        final Supplier<String> tokenGen = mock(Supplier.class);
        doThrow(exception).when(tokenGen).get();

        final var creds2 = jwtCredentials(tokenGen);
        creds2.applyRequestMetadata(requestInfo, executor, applier);

        final ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
        verify(applier, times(1)).fail(statusCaptor.capture());
        assertThat(statusCaptor.getValue().getCode(), equalTo(Status.UNAUTHENTICATED.getCode()));
        assertThat(statusCaptor.getValue().getCause(), equalTo(exception));
    }

    private void validateHeader(final Metadata metadata) {
        final String auth = metadata.get(JwtCallCredentials.AUTH_KEY);
        assertThat(auth, startsWith("Bearer "));
        final DecodedJWT decoded = JWT.decode(auth.substring(7)); // string "Bearer "
        assertThat(decoded.getIssuer(), equalTo("Globo-Chem"));
        assertThat(decoded.getSubject(), equalTo("Pit Pat"));
        assertThat(decoded.getIssuer(), equalTo("Globo-Chem"));
        final Algorithm algorithm = Algorithm.HMAC256("bag-hutch");
        JWT.require(algorithm).withIssuer("Globo-Chem").build().verify(decoded);
    }
}
