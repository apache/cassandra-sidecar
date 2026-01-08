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

import java.util.Locale;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request payload for stopping compaction operations.
 *
 * <p>Valid JSON:</p>
 * <pre>
 *   { "compactionType": "COMPACTION", "compactionId": "abc-123" }
 *   { "compactionType": "VALIDATION" }
 * </pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CompactionStopRequestPayload
{
    public static final String COMPACTION_TYPE_KEY = "compactionType";
    public static final String COMPACTION_ID_KEY = "compactionId";

    private final String compactionType;
    private final String compactionId;

    /**
     * Creates a new CompactionStopRequestPayload
     *
     * @param compactionType the type of compaction to stop (e.g., COMPACTION, VALIDATION, etc.)
     * @param compactionId   optional ID of a specific compaction to stop
     */
    @JsonCreator
    public CompactionStopRequestPayload(@JsonProperty(value = COMPACTION_TYPE_KEY) String compactionType,
                                        @JsonProperty(value = COMPACTION_ID_KEY) String compactionId)
    {
        // Normalize compactionType: trim whitespace and convert to uppercase
        this.compactionType = normalizeCompactionType(compactionType);
        this.compactionId = compactionId;
    }

    /**
     * Normalizes the compaction type by trimming whitespace and converting to uppercase.
     * Returns null for null or empty strings.
     *
     * @param compactionType the raw compaction type string
     * @return normalized compaction type or null
     */
    private static String normalizeCompactionType(String compactionType)
    {
        if (compactionType == null || compactionType.trim().isEmpty())
        {
            return null;
        }
        return compactionType.trim().toUpperCase(Locale.ROOT);
    }

    /**
     * @return the type of compaction to stop
     */
    @JsonProperty(COMPACTION_TYPE_KEY)
    public String compactionType()
    {
        return this.compactionType;
    }

    /**
     * @return the ID of a specific compaction to stop, or null to stop all specified type
     */
    @JsonProperty(COMPACTION_ID_KEY)
    public String compactionId()
    {
        return this.compactionId != null ? this.compactionId.trim() : null;
    }

    /**
     * Checks compaction ID valid - not null or empty post-trim
     * */
    public boolean hasValidCompactionId()
    {
        return this.compactionId != null && !this.compactionId.trim().isEmpty();
    }

    /**
     * Checks compaction type not null and not empty for invalid compactionId cases
    * */
    public boolean hasValidCompactionType()
    {
        return this.compactionType != null && !this.compactionType.isEmpty();
    }

    /**
     * Checks at least one valid parameter provided
     *
     * @return true if either compaction ID or compaction type is valid, false otherwise
     */
    public boolean atLeastOneParamProvided()
    {
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
