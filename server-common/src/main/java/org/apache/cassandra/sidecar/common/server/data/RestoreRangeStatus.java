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

package org.apache.cassandra.sidecar.common.server.data;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.sidecar.common.utils.Preconditions;

/**
 * Holds all possible restore range statuses
 */
public enum RestoreRangeStatus
{
    SUCCEEDED,
    /**
     * Failed is caused by any unrecoverable exception during processing the range
     */
    FAILED,
    /**
     * Aborted is caused by controller command
     */
    ABORTED,
    STAGED(SUCCEEDED, FAILED, ABORTED),
    CREATED(STAGED, FAILED, ABORTED);

    // Do not use EnumSet, since validTargetStatuses is assigned on constructing and enums are not available yet.
    private final Set<RestoreRangeStatus> validTargetStatusSet;

    RestoreRangeStatus(RestoreRangeStatus... targetStatuses)
    {
        this.validTargetStatusSet = new HashSet<>();
        Collections.addAll(validTargetStatusSet, targetStatuses);
    }

    /**
     * Advance the status with validation
     * @param targetStatus target status to advance to
     * @return new status
     */
    public RestoreRangeStatus advanceTo(RestoreRangeStatus targetStatus)
    {
        Preconditions.checkArgument(validTargetStatusSet.contains(targetStatus),
                                    name() + " status can only advance to one of the follow statuses: " +
                                    validTargetStatusSet);
        return targetStatus;
    }
}
