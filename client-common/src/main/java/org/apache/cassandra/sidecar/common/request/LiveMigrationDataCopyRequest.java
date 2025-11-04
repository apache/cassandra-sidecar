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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request payload for Live Migration data copy operations.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LiveMigrationDataCopyRequest
{

    /**
     * Maximum number of download iterations to attempt (must be &gt; 0).
     */
    public final int maxIterations;

    /**
     * Success threshold as a percentage (0.0 to 1.0) of data that must match.
     */
    public final double successThreshold;

    /**
     * Maximum number of files to download concurrently (must be &gt; 0).
     */
    public final int maxConcurrency;

    /**
     * Creates a new request with auto-generated ID.
     */
    @JsonCreator
    public LiveMigrationDataCopyRequest(@JsonProperty("maxIterations") int maxIterations,
                                        @JsonProperty("successThreshold") double successThreshold,
                                        @JsonProperty("maxConcurrency") int maxConcurrency)
    {

        if (maxIterations <= 0)
        {
            throw new IllegalArgumentException("Invalid maxIterations " + maxIterations +
                                               ". It cannot be less than or equal to zero.");
        }

        if (successThreshold < 0 || successThreshold > 1)
        {
            throw new IllegalArgumentException("Invalid successThreshold " + successThreshold +
                                               ". It cannot be less than zero or greater than one.");
        }

        if (maxConcurrency <= 0)
        {
            throw new IllegalArgumentException("Invalid maxConcurrency " + maxConcurrency
                                               + ". It cannot be less than or equal to zero.");
        }

        this.maxIterations = maxIterations;
        this.successThreshold = successThreshold;
        this.maxConcurrency = maxConcurrency;
    }
}
