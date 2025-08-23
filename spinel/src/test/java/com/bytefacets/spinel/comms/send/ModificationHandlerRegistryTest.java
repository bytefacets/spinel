// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.comms.send;

import static com.bytefacets.spinel.comms.send.ModificationHandlerRegistry.modificationHandlerRegistry;
import static com.bytefacets.spinel.comms.subscription.ChangeDescriptor.change;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.bytefacets.spinel.comms.subscription.ModificationRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ModificationHandlerRegistryTest {
    private @Mock ModificationHandler handler;
    private final ModificationRequest request = change("my-thing", "do-it", new Object[] {1});
    private final ModificationHandlerRegistry registry = modificationHandlerRegistry();

    @Test
    void shouldInvokeRegisteredHandler() {
        registry.register("my-thing", handler);
        registry.add(request);
        verify(handler, times(1)).add(request);
    }

    @Test
    void shouldUnregisterHandler() {
        registry.register("my-thing", handler);
        registry.unregister("my-thing");
        registry.add(request);
        verify(handler, never()).add(any());
    }

    @Test
    void shouldReturnFailureWhenUnknownHandler() {
        registry.register("not-my-thing", handler);
        final ModificationResponse response = registry.add(request);
        verify(handler, never()).add(any());
        assertThat(response.success(), equalTo(false));
        assertThat(response.message(), containsString("my-thing"));
    }
}
