/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.data;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.common.data.QualifiedTableName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StreamSSTableComponentRequestTest
{
    @Test
    void failsWhenKeyspaceIsNull()
    {
        String keyspace = null;
        assertThatThrownBy(() -> new StreamSSTableComponentRequest(keyspace, "table", "snapshot", "component"))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("keyspace must not be null");
    }

    @Test
    void failsWhenTableNameIsNull()
    {
        assertThatThrownBy(() -> new StreamSSTableComponentRequest("ks", null, "snapshot", "component"))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("tableName must not be null");
    }

    @Test
    void failsWhenSnapshotNameIsNull()
    {
        assertThatThrownBy(() -> new StreamSSTableComponentRequest("ks", "table", null, "component.db"))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("snapshotName must not be null");
    }

    @Test
    void failsWhenComponentNameIsNull()
    {
        assertThatThrownBy(() -> new StreamSSTableComponentRequest("ks", "table", "snapshot", null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("componentName must not be null");
    }

    @Test
    void testValidRequest()
    {
        StreamSSTableComponentRequest req =
        new StreamSSTableComponentRequest("ks", "table", "snapshot", "data.db");

        assertThat(req.keyspace()).isEqualTo("ks");
        assertThat(req.tableName()).isEqualTo("table");
        assertThat(req.snapshotName()).isEqualTo("snapshot");
        assertThat(req.componentName()).isEqualTo("data.db");
        assertThat(req.secondaryIndexName()).isNull();
        assertThat(req.dataDirectoryIndex()).isNull();
        assertThat(req.toString()).isEqualTo("StreamSSTableComponentRequest{keyspace='ks', tableName='table', " +
                                             "snapshot='snapshot', secondaryIndexName='null', " +
                                             "componentName='data.db', dataDirectoryIndex='null'}");
    }

    @Test
    void testValidRequestWithIndexName()
    {
        StreamSSTableComponentRequest req =
        new StreamSSTableComponentRequest(new QualifiedTableName("ks", "table"), "snapshot", ".index", "data.db",
                                          null, null);

        assertThat(req.keyspace()).isEqualTo("ks");
        assertThat(req.tableName()).isEqualTo("table");
        assertThat(req.snapshotName()).isEqualTo("snapshot");
        assertThat(req.secondaryIndexName()).isEqualTo(".index");
        assertThat(req.componentName()).isEqualTo("data.db");
        assertThat(req.dataDirectoryIndex()).isNull();
        assertThat(req.toString()).isEqualTo("StreamSSTableComponentRequest{keyspace='ks', tableName='table', " +
                                             "snapshot='snapshot', secondaryIndexName='.index', " +
                                             "componentName='data.db', dataDirectoryIndex='null'}");
    }

    @Test
    void testValidRequestWithDataDirIndex()
    {
        StreamSSTableComponentRequest req =
        new StreamSSTableComponentRequest(new QualifiedTableName("ks", "table"), "snapshot", ".index", "data.db",
                                          null, 42);

        assertThat(req.keyspace()).isEqualTo("ks");
        assertThat(req.tableName()).isEqualTo("table");
        assertThat(req.snapshotName()).isEqualTo("snapshot");
        assertThat(req.secondaryIndexName()).isEqualTo(".index");
        assertThat(req.componentName()).isEqualTo("data.db");
        assertThat(req.dataDirectoryIndex()).isEqualTo(42);
        assertThat(req.toString()).isEqualTo("StreamSSTableComponentRequest{keyspace='ks', tableName='table', " +
                                             "snapshot='snapshot', secondaryIndexName='.index', " +
                                             "componentName='data.db', dataDirectoryIndex='42'}");
    }

    @Test
    void testValidRequestWithTableId()
    {
        StreamSSTableComponentRequest req =
        new StreamSSTableComponentRequest(new QualifiedTableName("ks", "table"), "snapshot", ".index", "data.db",
                                          "1245", 42);

        assertThat(req.keyspace()).isEqualTo("ks");
        assertThat(req.tableName()).isEqualTo("table");
        assertThat(req.snapshotName()).isEqualTo("snapshot");
        assertThat(req.secondaryIndexName()).isEqualTo(".index");
        assertThat(req.componentName()).isEqualTo("data.db");
        assertThat(req.tableId()).isEqualTo("1245");
        assertThat(req.dataDirectoryIndex()).isEqualTo(42);
        assertThat(req.toString()).isEqualTo("StreamSSTableComponentRequest{keyspace='ks', tableName='table', " +
                                             "snapshot='snapshot', secondaryIndexName='.index', " +
                                             "componentName='data.db', dataDirectoryIndex='42'}");
    }
}
