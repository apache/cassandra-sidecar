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

package org.apache.cassandra.sidecar.common.request.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.data.CompactionType;

/**
 * Request payload for stopping compaction operations.
 *
 * <p>Valid JSON:</p>
 * <pre>
 *   { "compaction_type": "COMPACTION", "compaction_id": "abc-123" }
 *   { "compaction_type": "VALIDATION" }
 * </pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CompactionStopRequestPayload
{
    private final CompactionType compactionType;
    private final String compactionId;

    /**
     * Creates a new CompactionStopRequestPayload
     *
     * @param compactionType the type of compaction to stop (e.g., COMPACTION, VALIDATION, etc.)
     * @param compactionId   optional ID of a specific compaction to stop
     */
    @JsonCreator
    public CompactionStopRequestPayload(
    @JsonProperty(value = "compaction_type") CompactionType compactionType,
    @JsonProperty(value = "compaction_id") String compactionId
    ) {
        this.compactionType = compactionType;
        this.compactionId = compactionId;
    }

    /**
     * @return the type of compaction to stop
     */
    @JsonProperty("compaction_type")
    public CompactionType compactionType()
    {
        return this.compactionType;
    }

    /**
     * @return the ID of a specific compaction to stop, or null to stop all specified type
     */
    @JsonProperty("compaction_id")
    public String compactionId()
    {
        return this.compactionId;
    }

    /**
     * Checks compaction ID valid - not null or empty post-trim
     * */
    public boolean hasValidCompactionId() {
        return compactionId != null && !compactionId.trim().isEmpty();
    }

    /**
     * Checks compaction type not null for invalid compactionId cases
    * */
    public boolean hasValidCompactionType() {
        return compactionType != null;
    }

    /**
     * Checks at least one valid parameter provided
     *
     * @return true if either compaction ID or compaction type is valid, false otherwise
     */
    public boolean atLeastOneParamProvided() {
        return hasValidCompactionId() || hasValidCompactionType();
    }

    @Override
    public String toString()
    {
        return "CompactionStopRequestPayload{" +
               "compactionType='" + compactionType + "'" +
               ", compactionId='" + compactionId + "'" +
               "}";
    }
}
