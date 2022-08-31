package org.apache.cassandra.sidecar.common.data;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

class DataTypeSerializationTest
{
    ObjectMapper mapper = new ObjectMapper();

    @Test
    void serializePrimitiveType() throws JsonProcessingException
    {
        com.datastax.driver.core.DataType sourceType = com.datastax.driver.core.DataType.bigint();
        String expected = "{\"name\":\"BIGINT\",\"frozen\":false,\"typeArguments\":[],\"collection\":false}";
        serializationHelper(sourceType, expected);
    }

    @Test
    void serializeMapType() throws JsonProcessingException
    {
        com.datastax.driver.core.DataType.CollectionType sourceType =
        com.datastax.driver.core.DataType.map(com.datastax.driver.core.DataType.uuid(),
                                              com.datastax.driver.core.DataType.varchar());
        String expected = "{\"name\":\"MAP\",\"frozen\":false,\"typeArguments\":[" +
                          "{\"name\":\"UUID\",\"frozen\":false,\"typeArguments\":[],\"collection\":false}," +
                          "{\"name\":\"VARCHAR\",\"frozen\":false,\"typeArguments\":[],\"collection\":false}" +
                          "],\"collection\":true}";
        serializationHelper(sourceType, expected);
    }

    @Test
    void serializeSetType() throws JsonProcessingException
    {
        com.datastax.driver.core.DataType.CollectionType sourceType =
        com.datastax.driver.core.DataType.set(com.datastax.driver.core.DataType.cboolean());
        String expected = "{\"name\":\"SET\",\"frozen\":false,\"typeArguments\":[" +
                          "{\"name\":\"BOOLEAN\",\"frozen\":false,\"typeArguments\":[],\"collection\":false}" +
                          "],\"collection\":true}";
        serializationHelper(sourceType, expected);
    }

    @Test
    void serializeFrozenSetType() throws JsonProcessingException
    {
        com.datastax.driver.core.DataType.CollectionType sourceType =
        com.datastax.driver.core.DataType.frozenSet(com.datastax.driver.core.DataType.date());
        String expected = "{\"name\":\"SET\",\"frozen\":true,\"typeArguments\":[" +
                          "{\"name\":\"DATE\",\"frozen\":false,\"typeArguments\":[],\"collection\":false}" +
                          "],\"collection\":true}";
        serializationHelper(sourceType, expected);
    }

    @Test
    void serializeListType() throws JsonProcessingException
    {
        com.datastax.driver.core.DataType.CollectionType sourceType =
        com.datastax.driver.core.DataType.list(com.datastax.driver.core.DataType.blob());
        String expected = "{\"name\":\"LIST\",\"frozen\":false,\"typeArguments\":[" +
                          "{\"name\":\"BLOB\",\"frozen\":false,\"typeArguments\":[],\"collection\":false}" +
                          "],\"collection\":true}";
        serializationHelper(sourceType, expected);
    }

    @Test
    void serializeCustomType() throws JsonProcessingException
    {
        com.datastax.driver.core.DataType.CustomType sourceType =
        com.datastax.driver.core.DataType.custom("org.apache.cassandra.db.marshal.DateType");
        String expected = "{\"name\":\"LIST\",\"frozen\":false,\"typeArguments\":[" +
                          "{\"name\":\"BLOB\",\"frozen\":false,\"typeArguments\":[],\"collection\":false}" +
                          "],\"collection\":true}";
        serializationHelper(sourceType, expected);
    }

    private void serializationHelper(com.datastax.driver.core.DataType sourceType, String expected)
    throws JsonProcessingException
    {
        DataType type = DataType.of(sourceType);
        String json = mapper.writeValueAsString(type);
        assertThat(json).isEqualTo(expected);
    }
}