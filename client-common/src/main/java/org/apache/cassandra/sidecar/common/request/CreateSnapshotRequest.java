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

package org.apache.cassandra.sidecar.common.request;

import io.netty.handler.codec.http.HttpMethod;
import org.jetbrains.annotations.Nullable;

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
     * @param snapshotTTL  an optional time to live option for the snapshot (available since Cassandra 4.1+)
     *                     The TTL option must specify the units, for example 2d represents a TTL for 2 days;
     *                     1h represents a TTL of 1 hour, etc. Valid units are {@code d}, {@code h}, {@code s},
     *                     {@code ms}, {@code us}, {@code µs}, {@code ns}, and {@code m}.
     */
    public CreateSnapshotRequest(String keyspace, String table, String snapshotName, @Nullable String snapshotTTL)
    {
        super(keyspace, table, snapshotName, false, snapshotTTL);
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
