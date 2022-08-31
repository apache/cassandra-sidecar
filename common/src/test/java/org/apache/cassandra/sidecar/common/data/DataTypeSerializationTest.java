package org.apache.cassandra.sidecar.common.data;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

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
        String expected = "{\"name\":\"CUSTOM\",\"frozen\":false," +
                          "\"typeArguments\":[]," +
                          "\"collection\":false," +
                          "\"customTypeClassName\":\"org.apache.cassandra.db.marshal.DateType\"}";
        serializationHelper(sourceType, expected);
    }

    @Test
    void deserializePrimitiveType() throws IOException
    {
        String json = "{\"name\":\"INET\",\"frozen\":false,\"typeArguments\":[],\"collection\":false}";
        DataType type = mapper.readValue(json, DataType.class);
        assertThat(type.getName()).isEqualTo("INET");
        assertThat(type.isFrozen()).isFalse();
        assertThat(type.getTypeArguments()).isEmpty();
        assertThat(type.isCollection()).isFalse();
    }

    @Test
    void deserializeMapType() throws IOException
    {
        String json = "{\"name\":\"MAP\",\"frozen\":false,\"typeArguments\":[" +
                      "{\"name\":\"UUID\",\"frozen\":false,\"typeArguments\":[],\"collection\":false}," +
                      "{\"name\":\"VARCHAR\",\"frozen\":false,\"typeArguments\":[],\"collection\":false}" +
                      "],\"collection\":true}";
        DataType type = mapper.readValue(json, DataType.class);
        assertThat(type.getName()).isEqualTo("MAP");
        assertThat(type.isFrozen()).isFalse();
        assertThat(type.getTypeArguments().size()).isEqualTo(2);
        assertThat(type.getTypeArguments().get(0).getName()).isEqualTo("UUID");
        assertThat(type.getTypeArguments().get(1).getName()).isEqualTo("VARCHAR");
        assertThat(type.isCollection()).isTrue();
    }

    @Test
    void deserializeSetType() throws IOException
    {
        String json = "{\"name\":\"SET\",\"frozen\":false,\"typeArguments\":[" +
                      "{\"name\":\"BOOLEAN\",\"frozen\":false,\"typeArguments\":[],\"collection\":false}" +
                      "],\"collection\":true}";
        DataType type = mapper.readValue(json, DataType.class);
        assertThat(type.getName()).isEqualTo("SET");
        assertThat(type.isFrozen()).isFalse();
        assertThat(type.getTypeArguments().size()).isEqualTo(1);
        assertThat(type.getTypeArguments().get(0).getName()).isEqualTo("BOOLEAN");
        assertThat(type.isCollection()).isTrue();
    }

    @Test
    void deserializeFrozenSetType() throws IOException
    {
        String json = "{\"name\":\"SET\",\"frozen\":true,\"typeArguments\":[" +
                      "{\"name\":\"DATE\",\"frozen\":false,\"typeArguments\":[],\"collection\":false}" +
                      "],\"collection\":true}";
        DataType type = mapper.readValue(json, DataType.class);
        assertThat(type.getName()).isEqualTo("SET");
        assertThat(type.isFrozen()).isTrue();
        assertThat(type.getTypeArguments().size()).isEqualTo(1);
        assertThat(type.getTypeArguments().get(0).getName()).isEqualTo("DATE");
        assertThat(type.isCollection()).isTrue();
    }

    @Test
    void deserializeListType() throws IOException
    {
        String json = "{\"name\":\"LIST\",\"frozen\":false,\"typeArguments\":[" +
                      "{\"name\":\"BLOB\",\"frozen\":false,\"typeArguments\":[],\"collection\":false}" +
                      "],\"collection\":true}";
        DataType type = mapper.readValue(json, DataType.class);
        assertThat(type.getName()).isEqualTo("LIST");
        assertThat(type.isFrozen()).isFalse();
        assertThat(type.getTypeArguments().size()).isEqualTo(1);
        assertThat(type.getTypeArguments().get(0).getName()).isEqualTo("BLOB");
        assertThat(type.isCollection()).isTrue();
    }

    @Test
    void deserializeCustomType() throws IOException
    {
        String json = "{\"name\":\"CUSTOM\",\"frozen\":false," +
                      "\"typeArguments\":[]," +
                      "\"collection\":false," +
                      "\"customTypeClassName\":\"org.apache.cassandra.db.marshal.DateType\"}";
        DataType type = mapper.readValue(json, DataType.class);
        assertThat(type.getName()).isEqualTo("CUSTOM");
        assertThat(type.isFrozen()).isFalse();
        assertThat(type.getTypeArguments()).isEmpty();
        assertThat(type.getCustomTypeClassName()).isEqualTo("org.apache.cassandra.db.marshal.DateType");
        assertThat(type.isCollection()).isFalse();
    }

    @Test
    void failToDeserializeTypeWithoutName()
    {
        String json = "{\"frozen\":false,\"typeArguments\":[],\"collection\":false}";
        assertThatExceptionOfType(IOException.class)
        .isThrownBy(() -> mapper.readValue(json, DataType.class))
        .withMessageContaining("the name of the DataType must be non-null");
    }

    @Test
    void failToDeserializePrimitiveTypeWithTypeArguments()
    {
        String json = "{\"name\":\"BLOB\",\"frozen\":false,\"typeArguments\":[" +
                      "{\"name\":\"BLOB\",\"frozen\":false,\"typeArguments\":[],\"collection\":false}" +
                      "],\"collection\":false}";
        assertThatExceptionOfType(IOException.class)
        .isThrownBy(() -> mapper.readValue(json, DataType.class))
        .withMessageContaining("typeArguments only need to be provided for collection data types");
    }

    @Test
    void failToDeserializeMapTypeWithWrongNumberOfTypeArguments()
    {
        String json = "{\"name\":\"MAP\",\"frozen\":false,\"typeArguments\":[" +
                      "{\"name\":\"UUID\",\"frozen\":false,\"typeArguments\":[],\"collection\":false}" +
                      "],\"collection\":true}";
        assertThatExceptionOfType(IOException.class)
        .isThrownBy(() -> mapper.readValue(json, DataType.class))
        .withMessageContaining("MAP data type takes 2 typeArguments");
    }

    @Test
    void failToDeserializeListTypeWithWrongNumberOfTypeArguments()
    {
        String json = "{\"name\":\"LIST\",\"frozen\":false,\"typeArguments\":[" +
                      "{\"name\":\"UUID\",\"frozen\":false,\"typeArguments\":[],\"collection\":false}," +
                      "{\"name\":\"VARCHAR\",\"frozen\":false,\"typeArguments\":[],\"collection\":false}" +
                      "],\"collection\":true}";
        assertThatExceptionOfType(IOException.class)
        .isThrownBy(() -> mapper.readValue(json, DataType.class))
        .withMessageContaining("LIST and SET data types take 1 typeArgument");
    }

    @Test
    void failToDeserializeSetTypeWithWrongNumberOfTypeArguments()
    {
        String json = "{\"name\":\"SET\",\"frozen\":false,\"typeArguments\":[" +
                      "{\"name\":\"UUID\",\"frozen\":false,\"typeArguments\":[],\"collection\":false}," +
                      "{\"name\":\"VARCHAR\",\"frozen\":false,\"typeArguments\":[],\"collection\":false}" +
                      "],\"collection\":true}";
        assertThatExceptionOfType(IOException.class)
        .isThrownBy(() -> mapper.readValue(json, DataType.class))
        .withMessageContaining("LIST and SET data types take 1 typeArgument");
    }

    void serializationHelper(com.datastax.driver.core.DataType sourceType, String expected)
    throws JsonProcessingException
    {
        DataType type = DataType.of(sourceType);
        String json = mapper.writeValueAsString(type);
        assertThat(json).isEqualTo(expected);
    }
}
