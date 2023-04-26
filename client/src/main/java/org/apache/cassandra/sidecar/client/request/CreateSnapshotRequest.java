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

package org.apache.cassandra.sidecar.client.request;

import io.netty.handler.codec.http.HttpMethod;

/**
 * Represents a request to create a snapshot
 */
public class CreateSnapshotRequest extends SnapshotRequest<Void>
{
    /**
     * Constructs a new request to create a snapshot with name {@code snapshotName} for the given {@code keyspace}
     * and {@code table}.
     *
     * @param keyspace     the keyspace in Cassandra
     * @param table        the table name in Cassandra
     * @param snapshotName the name of the snapshot
     */
    public CreateSnapshotRequest(String keyspace, String table, String snapshotName)
    {
        super(keyspace, table, snapshotName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HttpMethod method()
    {
        return HttpMethod.PUT;
    }
}
