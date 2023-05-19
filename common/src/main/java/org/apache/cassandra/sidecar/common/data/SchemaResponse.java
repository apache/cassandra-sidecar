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

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A class representing a response for the {@code SchemaRequest}.
 */
public class SchemaResponse
{
    private final String keyspace;
    private final String schema;

    /**
     * Constructs a {@link SchemaResponse} object with the given {@code schema}.
     *
     * @param schema the schema for all keyspaces
     */
    public SchemaResponse(String schema)
    {
        this(null, schema);
    }

    /**
     * Constructs a {@link SchemaResponse} object with the {@code schema} for the given {@code keyspace}.
     *
     * @param keyspace the keyspace in Cassandra
     * @param schema   the schema for the given {@code keyspace}
     */
    public SchemaResponse(@JsonProperty("keyspace") String keyspace,
                          @JsonProperty("schema") String schema)
    {
        this.keyspace = keyspace;
        this.schema = Objects.requireNonNull(schema, "schema must be non-null");
    }

    /**
     * @return the name of the Cassandra keyspace
     */
    @JsonProperty("keyspace")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String keyspace()
    {
        return keyspace;
    }

    /**
     * @return the string representing the schema for the response
     */
    @JsonProperty("schema")
    public String schema()
    {
        return schema;
    }
}
