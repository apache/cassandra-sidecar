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

package org.apache.cassandra.sidecar.common.response;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response object for live migration file verification operations, containing statistics about
 * files not found at source/target and digest mismatches during verification.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class LiveMigrationFilesVerificationResponse
{
    private final String id;
    private final String state;
    private final String source;
    private final int port;
    private final int filesNotFoundAtSource;
    private final int filesNotFoundAtDestination;
    private final int metadataMatched;
    private final int metadataMismatches;
    private final int digestMismatches;
    private final int digestVerificationFailures;
    private final int filesMatched;
    private final String digestAlgorithm;

    @JsonCreator
    public LiveMigrationFilesVerificationResponse(@JsonProperty("id") String id,
                                                  @JsonProperty("digestAlgorithm") String digestAlgorithm,
                                                  @JsonProperty("state") String state,
                                                  @JsonProperty("source") String source,
                                                  @JsonProperty("port") int port,
                                                  @JsonProperty("filesNotFoundAtSource") int filesNotFoundAtSource,
                                                  @JsonProperty("filesNotFoundAtDestination") int filesNotFoundAtDestination,
                                                  @JsonProperty("metadataMatched") int metadataMatched,
                                                  @JsonProperty("metadataMismatches") int metadataMismatches,
                                                  @JsonProperty("digestMismatches") int digestMismatches,
                                                  @JsonProperty("digestVerificationFailures") int digestVerificationFailures,
                                                  @JsonProperty("filesMatched") int filesMatched)
    {
        this.id = Objects.requireNonNull(id, "id of files verification task must be specified");
        this.digestAlgorithm = digestAlgorithm;
        this.state = Objects.requireNonNull(state, "state of files verification task must be specified");
        this.source = source;
        this.port = port;
        this.filesNotFoundAtSource = filesNotFoundAtSource;
        this.filesNotFoundAtDestination = filesNotFoundAtDestination;
        this.metadataMatched = metadataMatched;
        this.metadataMismatches = metadataMismatches;
        this.digestMismatches = digestMismatches;
        this.digestVerificationFailures = digestVerificationFailures;
        this.filesMatched = filesMatched;
    }

    @JsonProperty("id")
    public String id()
    {
        return id;
    }

    @JsonProperty("state")
    public String state()
    {
        return state;
    }

    @JsonProperty("digestAlgorithm")
    public String digestAlgorithm()
    {
        return digestAlgorithm;
    }

    @JsonProperty("source")
    public String source()
    {
        return source;
    }

    @JsonProperty("port")
    public int port()
    {
        return port;
    }

    @JsonProperty("filesNotFoundAtSource")
    public int filesNotFoundAtSource()
    {
        return filesNotFoundAtSource;
    }

    @JsonProperty("filesNotFoundAtDestination")
    public int filesNotFoundAtDestination()
    {
        return filesNotFoundAtDestination;
    }

    @JsonProperty("metadataMatched")
    public int metadataMatched()
    {
        return metadataMatched;
    }

    @JsonProperty("metadataMismatches")
    public int metadataMismatches()
    {
        return metadataMismatches;
    }

    @JsonProperty("digestMismatches")
    public int digestMismatches()
    {
        return digestMismatches;
    }

    @JsonProperty("digestVerificationFailures")
    public int digestVerificationFailures()
    {
        return digestVerificationFailures;
    }

    @JsonProperty("filesMatched")
    public int filesMatched()
    {
        return filesMatched;
    }

    /**
     * Determines whether the live migration file verification completed successfully.
     *
     * @return true if and only if the verification state is COMPLETED and there are no
     * files missing at source or destination, no metadata mismatches, no digest
     * mismatches, and no digest verification failures.
     */
    @JsonIgnore
    public boolean isVerificationSuccessful()
    {
        return "COMPLETED".equals(state)
               && filesNotFoundAtSource == 0
               && filesNotFoundAtDestination == 0
               && metadataMismatches == 0
               && digestMismatches == 0
               && digestVerificationFailures == 0;
    }

    @Override
    public String toString()
    {
        return "LiveMigrationFilesVerificationResponse{" +
               "id='" + id + '\'' +
               ", digestAlgorithm='" + digestAlgorithm + '\'' +
               ", state=" + state +
               ", source='" + source + '\'' +
               ", port=" + port +
               ", filesNotFoundAtSource=" + filesNotFoundAtSource +
               ", filesNotFoundAtDestination=" + filesNotFoundAtDestination +
               ", metadataMatched=" + metadataMatched +
               ", metadataMismatches=" + metadataMismatches +
               ", digestMismatches=" + digestMismatches +
               ", digestVerificationFailures=" + digestVerificationFailures +
               ", filesMatched=" + filesMatched +
               '}';
    }
}
