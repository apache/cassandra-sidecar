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

/**
 * A request for checking existence of keyspace and an optional table.
 */
public class KeyspaceRequest extends QualifiedTableName
{
    /**
     * Constructs a {@link KeyspaceRequest} with the optional {@code keyspace} and optional {@code tableName}.
     *
     * @param keyspace  the keyspace in Cassandra
     * @param tableName the table name in Cassandra
     */
    public KeyspaceRequest(String keyspace, String tableName)
    {
        super(keyspace, tableName, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "KeyspaceRequest{" +
               "keyspace='" + getKeyspace() + '\'' +
               ", tableName='" + getTableName() + '\'' +
               '}';
    }
}
