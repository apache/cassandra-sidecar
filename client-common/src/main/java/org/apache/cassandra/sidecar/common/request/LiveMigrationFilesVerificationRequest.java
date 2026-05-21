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
 * Request to verify file integrity during live migration by computing and comparing digests.
 * Supports configurable concurrency and digest algorithms.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LiveMigrationFilesVerificationRequest
{
    private final int maxConcurrency;
    private final String digestAlgorithm;

    @JsonCreator
    public LiveMigrationFilesVerificationRequest(@JsonProperty("maxConcurrency") int maxConcurrency,
                                                 @JsonProperty("digestAlgorithm") String digestAlgorithm)
    {
        if (maxConcurrency <= 0)
        {
            throw new IllegalArgumentException("maxConcurrency must be >= 1");
        }
        if (digestAlgorithm == null || digestAlgorithm.trim().isEmpty())
        {
            throw new IllegalArgumentException("digestAlgorithm cannot be null or empty");
        }

        this.maxConcurrency = maxConcurrency;
        this.digestAlgorithm = digestAlgorithm;
    }

    @JsonProperty("maxConcurrency")
    public int maxConcurrency()
    {
        return maxConcurrency;
    }

    @JsonProperty("digestAlgorithm")
    public String digestAlgorithm()
    {
        return digestAlgorithm;
    }

    @Override
    public String toString()
    {
        return "LiveMigrationFilesVerificationRequest{" +
               "maxConcurrency=" + maxConcurrency +
               ", digestAlgorithm='" + digestAlgorithm + '\'' +
               '}';
    }
}
