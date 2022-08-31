package org.apache.cassandra.sidecar.common.data;

import java.io.IOException;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

class ColumnSchemaSerializationTest
{
    ObjectMapper mapper = new ObjectMapper();
    DataType custom = new DataType("CUSTOM", false, Collections.emptyList(), false, "foo.Clazz");

    @Test
    void testSerialization() throws JsonProcessingException
    {
        ColumnSchema schema = new ColumnSchema("column_1", new DataType("BLOB"), false);
        String expected = "{\"name\":\"column_1\"," +
                          "\"type\":{\"name\":\"BLOB\",\"frozen\":false,\"typeArguments\":[],\"collection\":false}," +
                          "\"static\":false}";
        serializationHelper(schema, expected);


        schema = new ColumnSchema("column_2", custom, true);
        expected = "{\"name\":\"column_2\",\"type\":" +
                   "{\"name\":\"CUSTOM\",\"frozen\":false," +
                   "\"typeArguments\":[],\"collection\":false,\"customTypeClassName\":\"foo.Clazz\"}," +
                   "\"static\":true}";
        serializationHelper(schema, expected);
    }

    @Test
    void testDeserialization() throws IOException
    {
        String json = "{\"name\":\"column_1\"," +
                      "\"type\":{\"name\":\"BLOB\",\"frozen\":false,\"typeArguments\":[],\"collection\":false}," +
                      "\"static\":false}";
        ColumnSchema schema = mapper.readValue(json, ColumnSchema.class);
        assertThat(schema.getName()).isEqualTo("column_1");
        assertThat(schema.getType()).isEqualTo(new DataType("BLOB"));
        assertThat(schema.isStatic()).isFalse();

        json = "{\"name\":\"column_2\",\"type\":" +
               "{\"name\":\"CUSTOM\",\"frozen\":false," +
               "\"typeArguments\":[],\"collection\":false,\"customTypeClassName\":\"foo.Clazz\"}," +
               "\"static\":true}";
        schema = mapper.readValue(json, ColumnSchema.class);
        assertThat(schema.getName()).isEqualTo("column_2");
        assertThat(schema.getType()).isEqualTo(custom);
        assertThat(schema.isStatic()).isTrue();
    }

    void serializationHelper(ColumnSchema schema, String expected) throws JsonProcessingException
    {
        String json = mapper.writeValueAsString(schema);
        assertThat(json).isEqualTo(expected);
    }
}
