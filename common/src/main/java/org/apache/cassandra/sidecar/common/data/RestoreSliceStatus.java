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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.sidecar.common.utils.Preconditions;

/**
 * Holds all possible restore slice statues
 */
public enum RestoreSliceStatus
{
    SUCCEEDED,
    FAILED,
    ABORTED,
    COMMITTING(SUCCEEDED, FAILED, ABORTED),
    STAGED(COMMITTING, FAILED, ABORTED),
    PROCESSING(STAGED, FAILED, ABORTED),
    EMPTY(PROCESSING, FAILED, ABORTED);

    // Do not use EnumSet, since validTargetStatuses is assigned on constructing and enums are not available yet.
    private final Set<RestoreSliceStatus> validTargetStatuses;

    RestoreSliceStatus(RestoreSliceStatus... targetStatuses)
    {
        this.validTargetStatuses = new HashSet<>();
        Collections.addAll(validTargetStatuses, targetStatuses);
    }

    /**
     * Advance the status with validation
     * @param targetStatus target status to advance to
     * @return new status
     */
    public RestoreSliceStatus advanceTo(RestoreSliceStatus targetStatus)
    {
        Preconditions.checkArgument(validTargetStatuses.contains(targetStatus),
                                    name() + " status can only advance to one of the follow statuses: " +
                                    validTargetStatuses);
        return targetStatus;
    }
}
