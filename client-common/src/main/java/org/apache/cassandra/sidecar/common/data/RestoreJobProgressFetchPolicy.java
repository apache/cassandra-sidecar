/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.common.data;

import java.util.Locale;

/**
 * Defines fetch policies for restore job progress endpoint. See individual fetch policy for details.
 */
public enum RestoreJobProgressFetchPolicy
{
    /**
     * Check the progress on all slices/ranges until encountering the first failed. Only the first failed range is collected.
     * <p>
     * A job is succeeded, if the query with this fetch policy returns empty response, meaning no failed and no unsatisfied slices/ranges.
     */
    FIRST_FAILED,

    /**
     * Gathers all failed and pending/unsatisfied slices/ranges. The policy is used to retrieve the slices/ranges
     * failed to import with the specified consistency level _when a job has failed_
     * <p>
     * A pending/unsatisfied range means sidecar instances are still working on importing
     *
     */
    ALL_FAILED_AND_PENDING,

    /**
     * Gather the progress of every slice/range in the job.
     * The fetch policy is mostly used for debugging.
     */
    ALL;

    @Override
    public String toString()
    {
        return name().toLowerCase(Locale.ROOT);
    }

    /**
     * @return {@link RestoreJobProgressFetchPolicy} from string
     */
    public static RestoreJobProgressFetchPolicy fromString(String name)
    {
        try
        {
            return valueOf(name.toUpperCase(Locale.ROOT));
        }
        catch (IllegalArgumentException unknownEnum)
        {
            throw new IllegalArgumentException("No RestoreJobProgressFetchPolicy found for " + name);
        }
    }
}
