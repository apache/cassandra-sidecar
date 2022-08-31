package org.apache.cassandra.sidecar.common.data;

import java.io.IOException;
import java.util.Collections;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

class KeyspaceSchemaSerializationTest
{
    ObjectMapper mapper = new ObjectMapper();

    @Test
    void testSerialization() throws JsonProcessingException
    {
        KeyspaceSchema keyspaceSchema = new KeyspaceSchema("ks",
                                                           Collections.emptyList(),
                                                           ImmutableMap.of("DC1", "3", "DC2", "4"),
                                                           true,
                                                           false);
        String expected = "{\"name\":\"ks\",\"tables\":[]," +
                          "\"replication\":{\"DC1\":\"3\",\"DC2\":\"4\"},\"durableWrites\":true,\"virtual\":false}";
        serializationHelper(keyspaceSchema, expected);
    }

    @Test
    void testDeserialization() throws IOException
    {
        String json = "{\"name\":\"ks\",\"tables\":[]," +
                      "\"replication\":{\"DC1\":\"3\",\"DC2\":\"4\"},\"durableWrites\":true,\"virtual\":false}";
        KeyspaceSchema schema = mapper.readValue(json, KeyspaceSchema.class);
        assertThat(schema.getName()).isEqualTo("ks");
        assertThat(schema.getTables()).isEmpty();
        assertThat(schema.getReplication()).isNotEmpty();
        assertThat(schema.getReplication()).containsEntry("DC1", "3");
        assertThat(schema.getReplication()).containsEntry("DC2", "4");
        assertThat(schema.isDurableWrites()).isTrue();
        assertThat(schema.isVirtual()).isFalse();
    }

    void serializationHelper(KeyspaceSchema schema, String expected) throws JsonProcessingException
    {
        String json = mapper.writeValueAsString(schema);
        assertThat(json).isEqualTo(expected);
    }
}
