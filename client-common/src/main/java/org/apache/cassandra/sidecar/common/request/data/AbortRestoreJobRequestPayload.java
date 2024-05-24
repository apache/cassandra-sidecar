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

import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.JOB_OPERATION_REASON;

/**
 * Request payload for aborting a restore job.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AbortRestoreJobRequestPayload
{
    private static final Pattern ALPHANUMERIC_BLANK_ONLY = Pattern.compile("^[a-zA-Z0-9 ]*$");
    private final String reason;

    @JsonCreator
    public AbortRestoreJobRequestPayload(@Nullable @JsonProperty(JOB_OPERATION_REASON) String reason)
    {
        this.reason = validateContent(reason);
    }

    /**
     * @return the reason to abort the job
     */
    @JsonProperty(JOB_OPERATION_REASON)
    public String reason()
    {
        return reason;
    }

    /**
     * As reason string is logged and persisted, the validation is performed to avoid any malicious behavior
     * @param reason client-sent string content
     * @return the same string if content is good
     */
    private String validateContent(String reason)
    {
        if (reason == null)
        {
            return null;
        }

        Preconditions.checkArgument(reason.length() <= 1024, "Reason string is too long");
        Preconditions.checkArgument(ALPHANUMERIC_BLANK_ONLY.matcher(reason).matches(),
                                    "Reason string cannot contain non-alphanumeric-blank characters");
        return reason;
    }
}
