// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.diaspore.common.jexl;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.jexl3.JexlArithmetic;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlFeatures;
import org.apache.commons.jexl3.introspection.JexlPermissions;
import org.apache.commons.jexl3.introspection.JexlSandbox;

/** Provides a default JexlEngine */
public final class JexlEngineProvider {
    private static final AtomicReference<JexlEngine> defaultEngine =
            new AtomicReference<>(createJexlEngine());

    private JexlEngineProvider() {}

    /** Overrides the default engine. */
    public static void setDefaultEngine(final JexlEngine engine) {
        defaultEngine.set(engine);
    }

    public static JexlEngine defaultJexlEngine() {
        return defaultEngine.get();
    }

    /**
     * Provides a basic JexlBuilder which is ready to create an engine that is pretty locked down
     * exception with access to Math and SafeTime static methods.
     *
     * <ul>
     *   <li>no loops
     *   <li>no side-effects
     *   <li>Jexl RESTRICTED permissions (blocks most of io/runtime/system, etc)
     *   <li>strict arithmetic (fail on null/invalid types instead of coercing)
     *   <li>not strict boolean logic (truthy non-booleans)
     *   <li>not silent - errors are thrown
     * </ul>
     */
    public static JexlBuilder defaultEngineBuilder() {
        // 1) Feature lockdown: disable loops and side-effects (no while/for, no global side
        // effects)
        final JexlFeatures features =
                new JexlFeatures()
                        .loops(false)
                        .sideEffect(false) // no mutation through expressions
                        .sideEffectGlobal(false); // avoid global side effects

        // 3) Create a Sandbox and whitelist only the classes + method names you actually need.
        final JexlSandbox sandbox = new JexlSandbox(false);
        sandbox.allow(Math.class.getName()); // allow all Math methods
        sandbox.allow(SafeTime.class.getName()); // allow all SafeTime methods
        sandbox.allow(String.class.getName()); // allow all String methods

        // because of the way the namespace static methods are resolved, it appears we need to
        // separately permit the class here (bc within the SandboxUberspect, the getConstructor
        // relies on a delegate Uberspect which is built using the permissions that are separate
        // from the sandbox
        final JexlPermissions perms = new JexlPermissions.ClassPermissions(SafeTime.class);

        // 5) Build the engine
        return new JexlBuilder()
                .features(features)
                .permissions(perms)
                .namespaces(createNamespaces())
                .sandbox(sandbox)
                .arithmetic(new JexlArithmetic(true))
                .strict(true)
                .silent(false);
    }

    public static JexlEngine createJexlEngine() {
        return defaultEngineBuilder().create();
    }

    private static Map<String, Object> createNamespaces() {
        // all static Math methods available and now()
        return Map.of("math", Math.class, "time", SafeTime.class);
    }
}
