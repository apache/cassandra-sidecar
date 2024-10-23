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

package org.apache.cassandra.sidecar.restore;

import org.apache.cassandra.sidecar.common.data.ConsistencyVerificationResult;
import org.apache.cassandra.sidecar.db.RestoreRange;

/**
 * Collects the progress of each {@link RestoreRange} and composes the overall {@link RestoreJobProgress}
 */
public interface RestoreJobProgressCollector
{
    /**
     * @return true if the collector can collect from more ranges; otherwise, false and the collector stops
     */
    boolean canCollectMore();

    /**
     * Collect the progress of the range
     *
     * @param range a range to be restored
     * @param checkResult result of the consistency check
     */
    void collect(RestoreRange range, ConsistencyVerificationResult checkResult);

    /**
     * Produce the {@link RestoreJobProgress} from the collected per range progress
     * @return restore job progress
     */
    RestoreJobProgress toRestoreJobProgress();
}
