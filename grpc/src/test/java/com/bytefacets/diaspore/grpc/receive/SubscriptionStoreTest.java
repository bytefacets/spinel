package com.bytefacets.diaspore.grpc.receive;

import static com.bytefacets.diaspore.comms.SubscriptionConfig.subscriptionConfig;
import static com.bytefacets.diaspore.schema.FieldDescriptor.intField;
import static com.bytefacets.diaspore.schema.MatrixStoreFieldFactory.matrixStoreFieldFactory;
import static com.bytefacets.diaspore.table.IntIndexedTableBuilder.intIndexedTable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.bytefacets.diaspore.comms.ConnectionInfo;
import com.bytefacets.diaspore.comms.SubscriptionConfig;
import com.bytefacets.diaspore.grpc.proto.SubscriptionRequest;
import com.bytefacets.diaspore.grpc.send.GrpcEncoder;
import com.bytefacets.diaspore.grpc.send.SendPackageAccess;
import com.bytefacets.diaspore.schema.Schema;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SubscriptionStoreTest {
    private final SendPackageAccess sender = new SendPackageAccess();
    private final SubscriptionStore store =
            new SubscriptionStore(new ConnectionInfo("", URI.create("direct://test")));
    private final GrpcDecoder decoder =
            GrpcDecoder.grpcDecoder(new SchemaBuilder(matrixStoreFieldFactory(16, 16, i -> {})));
    private final SubscriptionConfig config =
            subscriptionConfig("test").setFields(List.of("a", "b")).build();
    private @Mock Consumer<SubscriptionRequest> consumer;

    @Test
    void shouldCreateSubscription() {
        final Subscription sub = store.createSubscription(10, decoder, config);
        assertThat(sub.config(), sameInstance(config));
        assertThat(store.get(10), sameInstance(sub));
    }

    @Test
    void shouldRemoveSubscription() {
        store.createSubscription(10, decoder, config);
        assertThat(store.numSubscriptions(), equalTo(1));
        store.remove(10);
        assertThat(store.numSubscriptions(), equalTo(0));
        assertThat(store.get(10), nullValue());
    }

    @Test
    void shouldResetAllSubscriptionStatuses() {
        final var subs =
                IntStream.range(0, 5)
                        .mapToObj(i -> store.createSubscription(i, decoder, config))
                        .peek(sub -> sub.requestSubscriptionIfNecessary(consumer))
                        .peek(sub -> assertThat(sub.isSubscribed(), equalTo(true)))
                        .toList();
        store.resetSubscriptionStatus();
        subs.forEach(sub -> assertThat(sub.isSubscribed(), equalTo(false)));
    }

    @SuppressWarnings("unchecked")
    @Test
    void shouldResubscribeToOnlyThoseSubscriptionsThatAreUnsubscribed() {
        final var subs =
                IntStream.range(10, 15)
                        .mapToObj(i -> store.createSubscription(i, decoder, config))
                        .peek(sub -> sub.requestSubscriptionIfNecessary(consumer))
                        .peek(sub -> assertThat(sub.isSubscribed(), equalTo(true)))
                        .toList();
        reset(consumer);
        subs.get(1).markUnsubscribed();
        subs.get(3).markUnsubscribed();
        // when
        store.resubscribe(consumer);
        // then
        final ArgumentCaptor<SubscriptionRequest> requestCaptor =
                ArgumentCaptor.forClass(SubscriptionRequest.class);
        verify(consumer, times(2)).accept(requestCaptor.capture());
        assertThat(
                requestCaptor.getAllValues().stream()
                        .map(SubscriptionRequest::getRefToken)
                        .toList(),
                containsInAnyOrder(subs.get(1).token(), subs.get(3).token()));
    }

    @Test
    void shouldRouteResponseToSubscription() {
        // given
        final GrpcEncoder encoder = sender.encoder(10);
        final Schema schema =
                intIndexedTable("table").addFields(intField("a"), intField("b")).build().schema();
        // when
        store.createSubscription(10, decoder, config);
        store.accept(encoder.encodeSchema(schema));
        // then
        final Schema receivedSchema = decoder.output().schema();
        assertThat(receivedSchema.name(), equalTo("table"));
        assertThat(receivedSchema.field("a"), notNullValue());
        assertThat(receivedSchema.field("b"), notNullValue());
    }

    @Test
    void shouldResubscribeToAllSubscriptions() {
        // given
        final Set<SubscriptionRequest> expected = new HashSet<>();
        for (int i = 0; i < 5; i++) {
            final GrpcDecoder decoder =
                    GrpcDecoder.grpcDecoder(
                            new SchemaBuilder(matrixStoreFieldFactory(16, 16, x -> {})));
            expected.add(store.createSubscription(i + 10, decoder, config).createRequest());
        }
        // when
        store.resubscribe(consumer);
        // then
        final ArgumentCaptor<SubscriptionRequest> requests =
                ArgumentCaptor.forClass(SubscriptionRequest.class);
        verify(consumer, times(5)).accept(requests.capture());
        requests.getAllValues()
                .forEach(observed -> assertThat(expected.remove(observed), equalTo(true)));
        assertThat(expected, empty());
    }

    @Test
    void shouldNotThrowWhenRemovingUnknownToken() {
        store.remove(8768);
    }

    @Test
    void shouldNotThrowWhenReceivingResponseForUnknownToken() {
        // given
        final GrpcDecoder mockDecoder = mock(GrpcDecoder.class);
        final GrpcEncoder encoder = sender.encoder(10);
        final Schema schema =
                intIndexedTable("table").addFields(intField("a"), intField("b")).build().schema();
        store.createSubscription(10, mockDecoder, config);
        // when
        store.remove(10);
        store.accept(encoder.encodeSchema(schema));
        // then
        verifyNoInteractions(mockDecoder);
    }
}
