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

import org.apache.cassandra.sidecar.common.data.Keyspace;
import org.jetbrains.annotations.Nullable;

/**
 * Holder class for the {@link org.apache.cassandra.sidecar.routes.SchemaHandler}
 * request parameters
 */
public class SchemaRequest
{
    private final Keyspace keyspace;

    /**
     * Constructs a {@link SchemaRequest} with the {@link org.jetbrains.annotations.Nullable} {@code keyspace}.
     *
     * @param keyspace the keyspace in Cassandra
     */
    public SchemaRequest(@Nullable String keyspace)
    {
        this(keyspace == null ? null : new Keyspace(keyspace));
    }

    /**
     * Constructs a {@link SchemaRequest} with the {@link org.jetbrains.annotations.Nullable} {@code keyspace}.
     *
     * @param keyspace the keyspace in Cassandra
     */
    public SchemaRequest(@Nullable Keyspace keyspace)
    {
        this.keyspace = keyspace;
    }

    /**
     * @return the keyspace in Cassandra
     */
    public Keyspace keyspace()
    {
        return keyspace;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "SchemaRequest{" +
               "keyspace='" + keyspace + '\'' +
               '}';
    }
}
