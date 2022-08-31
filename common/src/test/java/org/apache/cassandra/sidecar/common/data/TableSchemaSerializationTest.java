package org.apache.cassandra.sidecar.common.data;

import java.io.IOException;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

class TableSchemaSerializationTest
{
    ObjectMapper mapper = new ObjectMapper();

    @Test
    void testSerialization() throws JsonProcessingException
    {
        TableSchema schema = new TableSchema("ks", "tbl1", false, false,
                                             Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
                                             Collections.emptyList(), null);
        String expected = "{\"keyspaceName\":\"ks\",\"name\":\"tbl1\",\"virtual\":false,\"secondaryIndexes\":false," +
                          "\"partitionKey\":[],\"clusteringColumns\":[],\"clusteringOrder\":[],\"columns\":[]," +
                          "\"options\":null}";
        serializationHelper(schema, expected);
    }

    @Test
    void testDeserialization() throws IOException
    {
        String columnJson = "{\"name\":\"column_1\"," +
                            "\"type\":{\"name\":\"BLOB\",\"frozen\":false,\"typeArguments\":[],\"collection\":false}," +
                            "\"static\":false}";
        String json = "{\"keyspaceName\":\"ks\",\"name\":\"tbl1\",\"virtual\":false,\"secondaryIndexes\":false," +
                      "\"partitionKey\":[" + columnJson + "],\"clusteringColumns\":[" + columnJson + "]," +
                      "\"clusteringOrder\":[\"DESC\"],\"columns\":[" + columnJson + "]," +
                      "\"options\":null}";

        ColumnSchema columnSchema = new ColumnSchema("column_1", new DataType("BLOB"), false);

        TableSchema schema = mapper.readValue(json, TableSchema.class);
        assertThat(schema.getKeyspaceName()).isEqualTo("ks");
        assertThat(schema.getName()).isEqualTo("tbl1");
        assertThat(schema.isVirtual()).isFalse();
        assertThat(schema.hasSecondaryIndexes()).isFalse();
        assertThat(schema.getPartitionKey()).containsExactly(columnSchema);
        assertThat(schema.getClusteringColumns()).containsExactly(columnSchema);
        assertThat(schema.getClusteringOrder()).containsExactly("DESC");
        assertThat(schema.getColumns()).containsExactly(columnSchema);
        assertThat(schema.getColumn("column_1")).isEqualTo(columnSchema);
    }

    void serializationHelper(TableSchema schema, String expected) throws JsonProcessingException
    {
        String json = mapper.writeValueAsString(schema);
        assertThat(json).isEqualTo(expected);
    }
}
