// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.printer;

import static com.bytefacets.spinel.common.DefaultNameSupplier.resolveName;
import static com.bytefacets.spinel.transform.BuilderSupport.builderSupport;
import static com.bytefacets.spinel.transform.TransformContext.continuation;
import static java.util.Objects.requireNonNull;

import com.bytefacets.spinel.transform.BuilderSupport;
import com.bytefacets.spinel.transform.TransformContext;
import com.bytefacets.spinel.transform.TransformContinuation;
import jakarta.annotation.Nullable;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

public final class OutputLoggerBuilder {
    private final BuilderSupport<OutputLogger> builderSupport;
    private final TransformContext transformContext;
    private final String name;
    private Level logLevel = Level.TRACE;
    private Logger logger;
    private boolean enabled = true;

    private OutputLoggerBuilder(final String name) {
        this.name = requireNonNull(name);
        this.builderSupport = builderSupport(name, this::internalBuild);
        this.transformContext = null;
    }

    private OutputLoggerBuilder(final TransformContext context) {
        this.transformContext = requireNonNull(context, "transform context");
        this.name = context.name();
        this.builderSupport =
                context.createBuilderSupport(this::internalBuild, () -> getOrCreate().input());
    }

    public static OutputLoggerBuilder logger() {
        return logger((String) null);
    }

    public static OutputLoggerBuilder logger(final @Nullable String name) {
        return new OutputLoggerBuilder(resolveName("Logger", name));
    }

    public static OutputLoggerBuilder logger(final TransformContext transformContext) {
        return new OutputLoggerBuilder(transformContext);
    }

    public OutputLogger getOrCreate() {
        return builderSupport.getOrCreate();
    }

    public OutputLogger build() {
        return builderSupport.createOperator();
    }

    public TransformContinuation then() {
        return continuation(
                transformContext, builderSupport.transformNode(), () -> getOrCreate().output());
    }

    private OutputLogger internalBuild() {
        final var useLogger = logger != null ? logger : LoggerFactory.getLogger(name);
        final var method = logMethod();
        final var node = new OutputLogger(useLogger, method, logLevel);
        node.enabled(this.enabled);
        return node;
    }

    public OutputLoggerBuilder logLevel(final Level logLevel) {
        this.logLevel = requireNonNull(logLevel, "logLevel");
        return this;
    }

    public OutputLoggerBuilder enabled() {
        this.enabled = true;
        return this;
    }

    public OutputLoggerBuilder disabled() {
        this.enabled = false;
        return this;
    }

    public OutputLoggerBuilder withLogger(final @Nullable Logger logger) {
        this.logger = logger;
        return this;
    }

    private BiConsumer<Logger, String> logMethod() {
        return switch (logLevel) {
            case TRACE -> Logger::trace;
            case DEBUG -> Logger::debug;
            case INFO -> Logger::info;
            case WARN -> Logger::warn;
            case ERROR -> Logger::error;
        };
    }
}
