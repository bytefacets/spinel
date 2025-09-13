// SPDX-FileCopyrightText: Copyright (c) 2025 Byte Facets
// SPDX-License-Identifier: MIT
package com.bytefacets.spinel.nats.kv;

import static com.bytefacets.spinel.nats.kv.BucketUtil.SCHEMA_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.bytefacets.spinel.grpc.proto.FieldDefinition;
import com.bytefacets.spinel.grpc.proto.SchemaUpdate;
import com.bytefacets.spinel.schema.TypeId;
import io.nats.client.KeyValue;
import io.nats.client.api.KeyValueEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SchemaRegistryTest {
    private @Mock KeyValue kv;
    private @Mock DistributedCounter counter;
    private SchemaRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new SchemaRegistry(kv, counter);
    }

    @Test
    void shouldCollectSchemaUpdate() {
        final SchemaUpdate update = schemaUpdate("foo", "a", "b");
        registry.apply(entry(SCHEMA_PREFIX + "567", update, 677));
        assertThat(registry.lookup(567), equalTo(update));
    }

    @Test
    void shouldUpdateLatest() {
        final SchemaUpdate update567 = schemaUpdate("foo", "a", "b");
        final SchemaUpdate update568 = schemaUpdate("foo", "a", "b", "c");
        registry.apply(entry(SCHEMA_PREFIX + "567", update567, 677));
        registry.apply(entry(SCHEMA_PREFIX + "568", update568, 678));
        assertThat(registry.latest().schemaId(), equalTo(568));
        assertThat(registry.latest().encodedSchema(), equalTo(update568));
    }

    @Test
    void shouldNotUpdateLatestIfLowerRevision() {
        final SchemaUpdate update567 = schemaUpdate("foo", "a", "b");
        final SchemaUpdate update568 = schemaUpdate("foo", "a", "b", "c");
        registry.apply(entry(SCHEMA_PREFIX + "568", update568, 678));
        registry.apply(entry(SCHEMA_PREFIX + "567", update567, 677));
        assertThat(registry.latest().schemaId(), equalTo(568));
        assertThat(registry.latest().encodedSchema(), equalTo(update568));
    }

    @Test
    void shouldRegisterNewSchemaIfNew() throws Exception {
        final SchemaUpdate update567 = schemaUpdate("foo", "a", "b");
        registry.apply(entry(SCHEMA_PREFIX + "567", update567, 677));
        when(counter.increment()).thenReturn(568);

        final SchemaUpdate update568 = schemaUpdate("foo", "a", "b", "c");
        assertThat(registry.register(update568), equalTo(568));

        assertThat(registry.lookup(567), equalTo(update567));
        assertThat(registry.lookup(568), equalTo(update568));
    }

    @Test
    void shouldNotRegisterNewSchemaIfKnown() throws Exception {
        final SchemaUpdate update567 = schemaUpdate("foo", "a", "b");
        final SchemaUpdate update568 = schemaUpdate("foo", "a", "b", "c");
        registry.apply(entry(SCHEMA_PREFIX + "567", update567, 677));
        registry.apply(entry(SCHEMA_PREFIX + "568", update568, 678));

        assertThat(registry.register(update568), equalTo(568));
        verifyNoInteractions(kv);
        verifyNoInteractions(counter);
    }

    private SchemaUpdate schemaUpdate(final String name, final String... fieldNames) {
        final var builder = SchemaUpdate.newBuilder().setName(name);
        for (String f : fieldNames) {
            builder.addFields(
                    FieldDefinition.newBuilder().setName(f).setTypeId(TypeId.Int).build());
        }
        return builder.build();
    }

    private KeyValueEntry entry(final String key, final SchemaUpdate update, final long revision) {
        final KeyValueEntry entry = mock(KeyValueEntry.class);
        lenient().when(entry.getKey()).thenReturn(key);
        lenient().when(entry.getValue()).thenReturn(update.toByteArray());
        lenient().when(entry.getRevision()).thenReturn(revision);
        return entry;
    }
}
