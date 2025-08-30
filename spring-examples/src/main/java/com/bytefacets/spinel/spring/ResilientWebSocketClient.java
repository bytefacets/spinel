// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.spring;

import static com.bytefacets.spinel.comms.SubscriptionConfig.subscriptionConfig;
import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.comms.SubscriptionConfig;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;

public final class ResilientWebSocketClient {
    private static final Logger log = LoggerFactory.getLogger(ResilientWebSocketClient.class);
    private static final Duration HB_INTERVAL = Duration.ofSeconds(30);
    private static final Duration MIN_RETRY_INTERVAL = Duration.ofSeconds(2);
    private static final Duration MAX_RETRY_INTERVAL = Duration.ofSeconds(60);
    private static final byte[] HB = "hb".getBytes(StandardCharsets.UTF_8);

    private final SubscriptionConfig subscriptionConfig;
    private final WebSocketClient client = new ReactorNettyWebSocketClient();
    private final URI endpoint = URI.create("ws://localhost:8080/ws/spinel");

    // Flag to stop reconnect loop if service is shutting down
    private final AtomicBoolean running = new AtomicBoolean(true);

    public static void main(final String[] args) throws Exception {
        new ResilientWebSocketClient(subscriptionConfig("order-view").defaultAll().build()).start();
        Thread.currentThread().join();
    }

    public ResilientWebSocketClient(final SubscriptionConfig subscriptionConfig) {
        this.subscriptionConfig = requireNonNull(subscriptionConfig, "subscriptionConfig");
    }

    public void start() {
        // formatting:off
        Flux.defer(this::connect)
                // Force reconnect even on normal completion
                .onErrorResume(
                        err -> {
                            log.warn("Connection error: {}", err.getMessage());
                            return Mono.error(err);
                        })
                .then(Mono.error(new RuntimeException("Disconnected"))) // <- force retry on completion
                .retryWhen(retryConfiguration())
                .repeatWhen(repeat -> repeat.delayElements(MIN_RETRY_INTERVAL)) // also retry on clean close
                .takeWhile(ignored -> running.get())
                .subscribe(
                        null,
                        err -> log.warn("WebSocket loop terminated: {}", err.getMessage()),
                        () -> log.info("WebSocket client stopped"));
        // formatting:on
    }

    private Mono<Void> connect() {
        log.info("Connecting to {}", endpoint);

        // formatting:off
        return client.execute(
                endpoint,
                session -> {
                    // --- Outbound ---
                    final Flux<WebSocketMessage> heartbeats = heartbeats(session);
                    final Flux<WebSocketMessage> outboundMessages =
                            Flux.just(session.textMessage(subscriptionConfig.remoteOutputName()));

                    // Merge outbound messages + heartbeats
                    final Flux<WebSocketMessage> outbound = Flux.merge(outboundMessages, heartbeats);

                    // --- Inbound ---
                    final Flux<String> inbound =
                            session.receive()
                                    .map(WebSocketMessage::getPayloadAsText)
                                    .doOnNext(msg -> System.out.println("Received: " + msg))
                                    .doOnComplete(this::onClosed)
                                    .doOnError(this::onError);

                    return session.send(outbound).thenMany(inbound).then();
                });
        // formatting:on
    }

    private void onClosed() {
        log.info("Server closed connection");
    }

    private void onError(final Throwable throwable) {
        log.warn("Error occurred", throwable);
    }

    private void beforeRetry(final Retry.RetrySignal signal) {
        log.debug("Reconnecting after: {}", signal.failure().getMessage());
    }

    private RetryBackoffSpec retryConfiguration() {
        return Retry.backoff(Long.MAX_VALUE, MIN_RETRY_INTERVAL)
                .maxBackoff(MAX_RETRY_INTERVAL)
                .jitter(0.2)
                .doBeforeRetry(this::beforeRetry);
    }

    private Flux<WebSocketMessage> heartbeats(final WebSocketSession session) {
        // formatting:off
        return Flux.interval(HB_INTERVAL).map(i -> session.pingMessage(factory -> factory.wrap(HB)));
        // formatting:on
    }

    public void stop() {
        running.set(false);
    }
}
