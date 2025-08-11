package com.bytefacets.diaspore.common.jexl;

public final class SafeTime {
    private SafeTime() {}

    public static long now() {
        return System.currentTimeMillis();
    }
}
