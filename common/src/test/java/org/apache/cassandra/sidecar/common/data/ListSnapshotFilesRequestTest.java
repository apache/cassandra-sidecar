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

package org.apache.cassandra.sidecar.common.data;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.ext.web.handler.HttpException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.from;

class ListSnapshotFilesRequestTest
{
    @Test
    void testNullKeyspaceIsAllowed()
    {
        ListSnapshotFilesRequest request = new ListSnapshotFilesRequest(null, "table", "snapshot", false);

        assertThat(request.getKeyspace()).isNull();
        assertThat(request.getTableName()).isEqualTo("table");
        assertThat(request.getSnapshotName()).isEqualTo("snapshot");
        assertThat(request.includeSecondaryIndexFiles()).isFalse();
        assertThat(request.toString()).isEqualTo("ListSnapshotFilesRequest{keyspace='null', tableName='table', snapshotName='snapshot', includeSecondaryIndexFiles=false}");
    }

    @Test
    void failsWhenKeyspaceContainsInvalidCharacters()
    {
        assertThatThrownBy(() -> new ListSnapshotFilesRequest("i_❤_u", "table", "snapshot", false))
        .isInstanceOf(HttpException.class)
        .hasMessageContaining("Bad Request")
        .returns(HttpResponseStatus.BAD_REQUEST.code(), from(t -> ((HttpException) t).getStatusCode()))
        .returns("Invalid characters in keyspace: i_❤_u", from(t -> ((HttpException) t).getPayload()));
    }

    @Test
    void failsWhenKeyspaceContainsPathTraversalAttack()
    {
        assertThatThrownBy(() -> new ListSnapshotFilesRequest("../../../etc/passwd", "table", "snapshot", false))
        .isInstanceOf(HttpException.class)
        .hasMessageContaining("Bad Request")
        .returns(HttpResponseStatus.BAD_REQUEST.code(), from(t -> ((HttpException) t).getStatusCode()))
        .returns("Invalid characters in keyspace: ../../../etc/passwd", from(t -> ((HttpException) t).getPayload()));
    }

    @ParameterizedTest
    @ValueSource(strings = { "system_schema", "system_traces", "system_distributed", "system", "system_auth",
                             "system_views", "system_virtual_schema", "cie_internal_local", "cie_internal" })
    void failsWhenKeyspaceIsForbidden(String forbiddenKeyspace)
    {
        assertThatThrownBy(() -> new ListSnapshotFilesRequest(forbiddenKeyspace, "table", "snapshot", false))
        .isInstanceOf(HttpException.class)
        .hasMessageContaining("Forbidden")
        .returns(HttpResponseStatus.FORBIDDEN.code(), from(t -> ((HttpException) t).getStatusCode()))
        .returns("Forbidden keyspace: " + forbiddenKeyspace, from(t -> ((HttpException) t).getPayload()));
    }

    @Test
    void testNullTableNameIsAllowed()
    {
        ListSnapshotFilesRequest request = new ListSnapshotFilesRequest("ks", null, "snapshot", true);

        assertThat(request.getKeyspace()).isEqualTo("ks");
        assertThat(request.getTableName()).isNull();
        assertThat(request.getSnapshotName()).isEqualTo("snapshot");
        assertThat(request.includeSecondaryIndexFiles()).isTrue();
        assertThat(request.toString()).isEqualTo("ListSnapshotFilesRequest{keyspace='ks', tableName='null', snapshotName='snapshot', includeSecondaryIndexFiles=true}");
    }

    @Test
    void failsWhenTableNameContainsInvalidCharacters()
    {
        assertThatThrownBy(() -> new ListSnapshotFilesRequest("ks", "i_❤_u", "snapshot", false))
        .isInstanceOf(HttpException.class)
        .hasMessageContaining("Bad Request")
        .returns(HttpResponseStatus.BAD_REQUEST.code(), from(t -> ((HttpException) t).getStatusCode()))
        .returns("Invalid characters in table name: i_❤_u", from(t -> ((HttpException) t).getPayload()));
    }

    @Test
    void failsWhenTableNameContainsPathTraversalAttack()
    {
        assertThatThrownBy(() -> new ListSnapshotFilesRequest("ks", "../../../etc/passwd", "snapshot", false))
        .isInstanceOf(HttpException.class)
        .hasMessageContaining("Bad Request")
        .returns(HttpResponseStatus.BAD_REQUEST.code(), from(t -> ((HttpException) t).getStatusCode()))
        .returns("Invalid characters in table name: ../../../etc/passwd", from(t -> ((HttpException) t).getPayload()));
    }

    @Test
    void failsWhenSnapshotNameIsNull()
    {
        assertThatThrownBy(() -> new ListSnapshotFilesRequest("ks", "table", null, false))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("snapshotName must not be null");
    }

    @ParameterizedTest
    @ValueSource(strings = { "slash/is-not-allowed", "null-char\0-is-not-allowed", "../../../etc/passwd" })
    void failsWhenSnapshotNameContainsInvalidCharacters(String invalidFileName)
    {
        assertThatThrownBy(() -> new ListSnapshotFilesRequest("ks", "table", invalidFileName, false))
        .isInstanceOf(HttpException.class)
        .hasMessageContaining("Bad Request")
        .returns(HttpResponseStatus.BAD_REQUEST.code(), from(t -> ((HttpException) t).getStatusCode()))
        .returns("Invalid characters in snapshot name: " + invalidFileName, from(t -> ((HttpException) t).getPayload()));
    }

    @Test
    void testValidRequest()
    {
        ListSnapshotFilesRequest request = new ListSnapshotFilesRequest("ks", "table", "snapshot", false);

        assertThat(request.getKeyspace()).isEqualTo("ks");
        assertThat(request.getTableName()).isEqualTo("table");
        assertThat(request.getSnapshotName()).isEqualTo("snapshot");
        assertThat(request.includeSecondaryIndexFiles()).isFalse();
        assertThat(request.toString()).isEqualTo("ListSnapshotFilesRequest{keyspace='ks', tableName='table', snapshotName='snapshot', includeSecondaryIndexFiles=false}");
    }
}
