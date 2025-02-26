/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.datahub;

import java.io.IOException;
import java.io.StringReader;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.Session;
import com.linkedin.data.DataList;
import com.linkedin.data.codec.JacksonDataCodec;
import org.apache.cassandra.sidecar.common.server.utils.IOUtils;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.jetbrains.annotations.NotNull;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for {@link SchemaReporter}
 */
@SuppressWarnings("resource")
final class SchemaReporterIntegrationTest extends IntegrationTestBase
{
    private static final IdentifiersProvider IDENTIFIERS = new TestIdentifiers();
    private static final JacksonDataCodec CODEC = new JacksonDataCodec();

    /**
     * Private helper method that removes all numeric suffixes added non-deterministically
     * to the names of data centers, clusters, keyspaces, and tables during preparation
     */
    @NotNull
    private static String normalizeNames(@NotNull String schema)
    {
        return schema.replaceAll("(?is)(?<=\\b(" + DATA_CENTER_PREFIX + "|"
                                                 + TEST_CLUSTER_PREFIX + "|"
                                                 + TEST_KEYSPACE + "|"
                                                 + TEST_TABLE_PREFIX + "))\\d+\\b", "");
    }

    @CassandraIntegrationTest
    void testSchemaConverter() throws IOException
    {
        // Prepare a test keyspace, a test table, and a number of test UDTs;
        // the goal is to cover all supported data types and their combinations
        waitForSchemaReady(1L, TimeUnit.MINUTES);
        createTestKeyspace();
        createTestUdt("numbers",     "ti tinyint, "
                                   + "si smallint, "
                                   + "bi bigint, "
                                   + "vi varint, "
                                   + "sf float, "
                                   + "df double, "
                                   + "de decimal");
        createTestUdt("datetime",    "dd date, "
                                   + "ts timestamp, "
                                   + "tt time");
        createTestUdt("strings",     "tu timeuuid, "
                                   + "ru uuid, "
                                   + "ip inet, "
                                   + "as ascii, "
                                   + "us text, "
                                   + "vc varchar");
        createTestUdt("collections", "t tuple<int, ascii>, "
                                   + "s set<ascii>, "
                                   + "l frozen<list<ascii>>, "
                                   + "m map<ascii, frozen<map<ascii, int>>>");
        createTestUdt("types",       "b blob");
        createTestUdt("other",       "t frozen<types>");
        createTestTable("CREATE TABLE " + TEST_KEYSPACE + "." + TEST_TABLE_PREFIX + " ("
                      + "b boolean PRIMARY KEY, "
                      + "n numbers, "
                      + "t frozen<datetime>, "
                      + "s strings, "
                      + "c frozen<collections>, "
                      + "o other)" + WITH_COMPACTION_DISABLED + ";");

        // First, ensure the returned schema matches the reference one
        // (while ignoring name suffixes and whitespace characters)
        JsonEmitter emitter = new JsonEmitter();
        try (Session session = maybeGetSession())
        {
            new SchemaReporter(IDENTIFIERS, () -> emitter)
                    .process(session.getCluster());
        }
        String   actualJson = normalizeNames(emitter.content());
        String expectedJson = IOUtils.readFully("/datahub/integration_test.json");

        assertThat(actualJson)
                .isEqualToNormalizingWhitespace(expectedJson);

        // Finally, make sure the returned schema produces the same tree of
        // DataHub objects after having been normalized and deserialized
        DataList   actualData = CODEC.readList(new StringReader(actualJson));
        DataList expectedData = CODEC.readList(new StringReader(expectedJson));

        assertThat(actualData)
                .isEqualTo(expectedData);
    }
}
