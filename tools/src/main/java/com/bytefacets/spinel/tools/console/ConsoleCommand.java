// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.tools.console;

import static com.bytefacets.spinel.grpc.receive.GrpcClientBuilder.grpcClient;
import static com.bytefacets.spinel.grpc.receive.GrpcSourceBuilder.grpcSource;
import static com.bytefacets.spinel.grpc.receive.auth.JwtCallCredentials.jwtCredentials;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElseGet;
import static org.slf4j.Logger.ROOT_LOGGER_NAME;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.bytefacets.spinel.common.Connector;
import com.bytefacets.spinel.comms.ConnectionInfo;
import com.bytefacets.spinel.comms.SubscriptionConfig;
import com.bytefacets.spinel.grpc.receive.GrpcClient;
import com.bytefacets.spinel.grpc.receive.GrpcClientBuilder;
import com.bytefacets.spinel.grpc.receive.GrpcSource;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoop;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.fusesource.jansi.Ansi;
import org.fusesource.jansi.AnsiConsole;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(name = "console")
public final class ConsoleCommand implements Callable<Integer> {
    private static final String JWT_SECRET_ENV_NAME = "JWT_SECRET";
    private final EventLoop eventLoop;
    private final Map<String, String> env;
    private GrpcClient client;

    @CommandLine.Option(
            names = "--endpoint",
            description = "grpc endpoint, like '0.0.0.0:15000",
            required = true)
    String endpoint;

    @CommandLine.Option(names = "--output", description = "output name", required = true)
    String outputName;

    @CommandLine.Option(names = "--jwt-issuer", description = "jwt issuer")
    String jwtIssuer;

    @CommandLine.Option(names = "--jwt-user", description = "jwt user")
    String jwtUser;

    @CommandLine.Option(
            names = "--jwt-secret",
            description = "jwt secret or will look in JWT_SECRET env variable")
    String jwtSecret;

    // REVISIT field selection

    public ConsoleCommand() {
        this(null, System.getenv());
    }

    ConsoleCommand(final EventLoop eventLoop, final Map<String, String> env) {
        final LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger(ROOT_LOGGER_NAME).setLevel(Level.WARN);
        this.eventLoop = requireNonNullElseGet(eventLoop, DefaultEventLoop::new);
        this.env = requireNonNull(env, "env");
    }

    @Override
    public Integer call() {
        AnsiConsole.systemInstall();
        build();
        System.out.println(Ansi.ansi().eraseScreen());
        client.connect();
        try {
            Thread.currentThread().join();
        } catch (InterruptedException ex) {
            client.disconnect();
            eventLoop.close();
            System.out.println("Terminating");
        } finally {
            AnsiConsole.systemUninstall();
        }
        return 0;
    }

    private void build() {
        final ManagedChannel channel =
                ManagedChannelBuilder.forTarget(endpoint)
                        .usePlaintext()
                        .enableRetry()
                        .keepAliveTime(5, TimeUnit.MINUTES)
                        .keepAliveTimeout(20, TimeUnit.SECONDS)
                        .build();
        // formatting:off
        client = maybeWithJwt(grpcClient(channel, eventLoop)
                    .connectionInfo(new ConnectionInfo(endpoint, endpoint)))
                    .build();
        final GrpcSource source =
                grpcSource(client, endpoint)
                        .subscription(
                                SubscriptionConfig.subscriptionConfig(outputName)
                                        .defaultAll().build())
                        .build();
        // formatting:on
        Connector.connectInputToOutput(new ConsoleRenderer(), source);
    }

    GrpcClientBuilder maybeWithJwt(final GrpcClientBuilder builder) {
        final String secret = resolveJwtSecret(env);
        if (jwtIssuer != null && jwtUser != null && secret != null) {
            final var creds = jwtCredentials(jwtIssuer, jwtUser, jwtSecret);
            return builder.withSpecializer(stub -> stub.withCallCredentials(creds));
        } else {
            return builder;
        }
    }

    private String resolveJwtSecret(final Map<String, String> env) {
        return jwtSecret != null ? jwtSecret : env.get(JWT_SECRET_ENV_NAME);
    }
}
