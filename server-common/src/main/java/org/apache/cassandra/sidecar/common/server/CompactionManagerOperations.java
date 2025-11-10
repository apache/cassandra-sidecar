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

package org.apache.cassandra.sidecar.common.server;

import java.util.List;
import java.util.Map;

/**
 * An interface that defines the compaction manager operations exposed by Sidecar
 */
public interface CompactionManagerOperations
{
    /**
     * Returns active compactions as a list of compaction info maps
     *
     * @return list of compaction info maps
     */
    List<Map<String, String>> getCompactions();

    /**
     * Stops compaction based on compaction ID or type.
     * If compactionId is provided, it takes precedence over compactionType.
     *
     * @param compactionId   the compaction ID to stop (nullable)
     * @throws IllegalArgumentException if both parameters are null or empty
     */
    void stopCompactionById(String compactionId);

    /**
     * Stops compaction based on compaction ID or type.
     * If compactionId is provided, it takes precedence over compactionType.
     *
     * @param compactionType   the compaction ID to stop (nullable)
     * @throws IllegalArgumentException if both parameters are null or empty
     */
    void stopCompaction(String compactionType);
}
