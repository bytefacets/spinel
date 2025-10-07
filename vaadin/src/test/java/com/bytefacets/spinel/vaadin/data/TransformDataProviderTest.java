// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.vaadin.data;

import static com.bytefacets.spinel.comms.subscription.ModificationRequestFactory.applyFilterExpression;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.bytefacets.collections.functional.IntIntConsumer;
import com.bytefacets.spinel.TransformOutput;
import com.bytefacets.spinel.comms.send.ModificationResponse;
import com.bytefacets.spinel.comms.send.SubscriptionContainer;
import com.bytefacets.spinel.comms.subscription.ModificationRequest;
import com.vaadin.flow.data.provider.Query;
import io.netty.channel.EventLoop;
import java.util.Optional;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TransformDataProviderTest {
    private @Mock SubscriptionContainer subscription;
    private @Mock TransformOutput subscriptionOutput;
    private @Mock(strictness = Mock.Strictness.LENIENT) EventLoop eventLoop;
    private @Mock TransformConsumer eventLoopConsumer;
    private @Mock UIThreadConsumer uiThreadConsumer;
    private @Mock(strictness = Mock.Strictness.LENIENT) Query<TransformRow, ModificationRequest>
            query;
    private @Captor ArgumentCaptor<Runnable> runnableCaptor;
    private @Captor ArgumentCaptor<ModificationRequest> filterCaptor;
    private final ModificationRequest filter1 = applyFilterExpression("key == 1");
    private final ModificationRequest filter2 = applyFilterExpression("key == 2");
    private TransformDataProvider provider;

    @BeforeEach
    void setUp() {
        provider =
                new TransformDataProvider(
                        subscription, eventLoopConsumer, uiThreadConsumer, eventLoop);
        when(query.getFilter()).thenReturn(Optional.empty());
        lenient().when(subscription.output()).thenReturn(subscriptionOutput);
        lenient().when(subscription.add(any())).thenReturn(ModificationResponse.SUCCESS);
    }

    @Test
    void shouldConnectOnEventLoopThread() {
        verify(subscriptionOutput, never()).attachInput(any());
        runEventLoop();
        verify(subscriptionOutput, times(1)).attachInput(eventLoopConsumer);
    }

    @Test
    void shouldTerminateSubscriptionOnDisconnect() {
        reset(eventLoop);
        provider.disconnect();
        verify(subscription, never()).terminateSubscription();
        runEventLoop();
        verify(subscription, times(1)).terminateSubscription();
        verify(subscriptionOutput, times(1)).detachInput(eventLoopConsumer);
    }

    @Nested
    class SizeTests {
        @BeforeEach
        void setUp() {
            reset(eventLoop);
        }

        @Test
        void shouldApplyOnUiThread() {
            provider.size(query);
            verify(eventLoopConsumer, times(1)).applyOnUiThread();
        }

        @Test
        void shouldApplyFilter() {
            when(query.getFilter()).thenReturn(Optional.of(filter1));
            processEventLoopInLine();
            provider.size(query);
            verify(eventLoop, times(1)).execute(any());
            verify(subscription, times(1)).add(filter1);
        }

        @Test
        void shouldReturnRowCount() {
            when(uiThreadConsumer.rowCount()).thenReturn(10);
            assertThat(provider.size(query), equalTo(10));
        }
    }

    @Nested
    class FetchTests {
        @BeforeEach
        void setUp() {
            reset(eventLoop);
            processEventLoopInLine();
        }

        @Test
        void shouldApplyOnUiThread() {
            provider.fetch(query);
            verify(eventLoopConsumer, times(1)).applyOnUiThread();
        }

        @Test
        void shouldApplyFilter() {
            when(query.getFilter()).thenReturn(Optional.of(filter1));
            provider.fetch(query);
            verify(eventLoop, times(1)).execute(any());
            verify(subscription, times(1)).add(filter1);
        }

        @Test
        void shouldReturnRowsFromConsumer() {
            doAnswer(
                            inv -> {
                                final var consumer = inv.getArgument(2, IntIntConsumer.class);
                                IntStream.range(0, 3).forEach(i -> consumer.accept(i, i + 10));
                                return null;
                            })
                    .when(uiThreadConsumer)
                    .rowsInRange(anyInt(), anyInt(), any());
            final var results = provider.fetch(query).toList();
            assertThat(
                    results,
                    contains(new TransformRow(10), new TransformRow(11), new TransformRow(12)));
        }

        @Test
        void shouldCallWithQueryAndOffset() {
            when(query.getLimit()).thenReturn(100);
            when(query.getOffset()).thenReturn(50);
            provider.fetch(query);
            verify(uiThreadConsumer, times(1)).rowsInRange(eq(50), eq(100), any());
        }
    }

    @Nested
    class FilterTests {
        @BeforeEach
        void setUp() {
            reset(eventLoop);
            processEventLoopInLine();
        }

        @Test
        void shouldApplyFilterWhenNotYetApplied() {
            provider.applyFilterIfChanged(filter1);
            verify(eventLoop, times(1)).execute(any());
            verify(subscription, times(1)).add(filter1);
            assertThat(provider.currentFilter(), equalTo(filter1));
        }

        @Test
        void shouldNotApplyFilterWhenAlreadyApplied() {
            provider.applyFilterIfChanged(filter1);
            provider.applyFilterIfChanged(filter1);
            // we only ran once
            verify(eventLoop, times(1)).execute(any());
            verify(subscription, times(1)).add(filter1);
            assertThat(provider.currentFilter(), equalTo(filter1));
        }

        @Test
        void shouldReplaceFilterWhenChanged() {
            provider.applyFilterIfChanged(filter1);
            reset(subscription);
            when(subscription.add(any())).thenReturn(ModificationResponse.SUCCESS);
            provider.applyFilterIfChanged(filter2);
            verify(subscription, times(1)).add(filter2);
            verify(subscription, times(1)).remove(filter1);
            assertThat(provider.currentFilter(), equalTo(filter2));
        }

        @Test
        void shouldRemoveFilterWhenNulled() {
            provider.applyFilterIfChanged(filter1);
            reset(subscription);
            provider.applyFilterIfChanged(null);
            verify(subscription, never()).add(any());
            verify(subscription, times(1)).remove(filter1);
        }

        @Test
        void shouldNullOutFilterWhenModificationResponseIsFailed() {
            when(subscription.add(any()))
                    .thenReturn(ModificationResponse.MODIFICATION_NOT_UNDERSTOOD);
            provider.applyFilterIfChanged(filter1);
            assertThat(provider.currentFilter(), nullValue());
        }
    }

    private void processEventLoopInLine() {
        doAnswer(
                        inv -> {
                            inv.getArgument(0, Runnable.class).run();
                            return null;
                        })
                .when(eventLoop)
                .execute(any());
    }

    private void runEventLoop() {
        verify(eventLoop, times(1)).execute(runnableCaptor.capture());
        runnableCaptor.getValue().run();
    }
}
