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

package org.apache.cassandra.sidecar.cluster;

import java.util.Set;

import org.apache.cassandra.sidecar.cluster.locator.InstanceSetByDc;
import org.jetbrains.annotations.NotNull;

/**
 * Verifier to check the progress with the consistency level
 */
public interface ConsistencyVerifier
{
    /**
     * Verify the current progress and conclude a result
     *
     * @param succeeded current instances succeed
     * @param failed    current instances fail
     * @param all       all participant instances considering the replication strategy;
     *                  instances are grouped by dc
     * @return result
     */
    Result verify(@NotNull Set<String> succeeded, @NotNull Set<String> failed, @NotNull InstanceSetByDc all);

    /**
     * Verification result
     */
    enum Result
    {
        SATISFIED,  // the passed replicas have satisfied the consistency level
        PENDING,    // no conclusion can be made yet
        FAILED,     // the failed replicas have failed the consistency level
    }
}
