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

import org.apache.cassandra.sidecar.routes.data.SnapshotRequestParam;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for the {@link SnapshotRequestParam} object
 */
class SnapshotRequestParamTest
{
    @Test
    void failsWhenSnapshotNameIsNull()
    {
        assertThatThrownBy(() -> SnapshotRequestParam.builder().qualifiedTableName("ks", "table").build())
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("snapshotName must not be null");
    }

    @Test
    void testValidRequest()
    {
        SnapshotRequestParam request = SnapshotRequestParam.builder().qualifiedTableName("ks", "table")
                                                           .snapshotName("snapshot")
                                                           .build();

        assertThat(request.qualifiedTableName()).isNotNull();
        assertThat(request.qualifiedTableName().keyspace()).isEqualTo("ks");
        assertThat(request.qualifiedTableName().tableName()).isEqualTo("table");
        assertThat(request.keyspace()).isEqualTo("ks");
        assertThat(request.tableName()).isEqualTo("table");
        assertThat(request.snapshotName()).isEqualTo("snapshot");
        assertThat(request.includeSecondaryIndexFiles()).isFalse();
        assertThat(request.ttl()).isNull();
        assertThat(request.toString()).isEqualTo("SnapshotRequest{keyspace='ks', tableName='table', " +
                                                 "snapshotName='snapshot', includeSecondaryIndexFiles=false, " +
                                                 "ttl=null}");
    }

    @Test
    void testValidRequestWithTTL()
    {
        SnapshotRequestParam request = SnapshotRequestParam.builder().qualifiedTableName("ks", "table")
                                                           .snapshotName("snapshot")
                                                           .ttl("3d")
                                                           .build();

        assertThat(request.qualifiedTableName()).isNotNull();
        assertThat(request.qualifiedTableName().keyspace()).isEqualTo("ks");
        assertThat(request.qualifiedTableName().tableName()).isEqualTo("table");
        assertThat(request.keyspace()).isEqualTo("ks");
        assertThat(request.tableName()).isEqualTo("table");
        assertThat(request.snapshotName()).isEqualTo("snapshot");
        assertThat(request.includeSecondaryIndexFiles()).isFalse();
        assertThat(request.ttl()).isEqualTo("3d");
        assertThat(request.toString()).isEqualTo("SnapshotRequest{keyspace='ks', tableName='table', " +
                                                 "snapshotName='snapshot', includeSecondaryIndexFiles=false, " +
                                                 "ttl=3d}");
    }
}
