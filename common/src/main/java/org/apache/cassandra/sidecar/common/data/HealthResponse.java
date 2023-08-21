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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.NotNull;

/**
 * A class representing a response for a health request
 * (either {@code SidecarHealthRequest} or {@code CassandraHealthRequest})
 */
public class HealthResponse
{
    @NotNull
    private final String status;

    /**
     * Constructs a {@link HealthResponse} object with the given {@code status}
     *
     * @param status the status
     */
    public HealthResponse(@NotNull @JsonProperty("status") String status)
    {
        this.status = Objects.requireNonNull(status, "status must not be null").toUpperCase();
    }

    /**
     * @return the status
     */
    @JsonProperty("status")
    public String status()
    {
        return status;
    }

    /**
     * @return whether the status is OK
     */
    @JsonIgnore
    public boolean isOk()
    {
        return status.equals("OK");
    }
}
