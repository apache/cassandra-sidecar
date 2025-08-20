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

package org.apache.cassandra.sidecar.common.response.data;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents pending compaction tasks by keyspace and table
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PendingCompactionTasks
{
    private final Map<String, Map<String, Integer>> pendingTasksByTable;
    private final int totalPendingTasks;

    /**
     * Constructs a new {@link PendingCompactionTasks}.
     *
     * @param pendingTasksByTable pending tasks organized by keyspace and table
     * @param totalPendingTasks   total count of pending tasks
     */
    @JsonCreator
    public PendingCompactionTasks(@JsonProperty("pendingTasksByTable") Map<String, Map<String, Integer>> pendingTasksByTable,
                                  @JsonProperty("totalPendingTasks") int totalPendingTasks)
    {
        this.pendingTasksByTable = pendingTasksByTable;
        this.totalPendingTasks = totalPendingTasks;
    }

    @JsonProperty("pendingTasksByTable")
    public Map<String, Map<String, Integer>> pendingTasksByTable()
    {
        return pendingTasksByTable;
    }

    @JsonProperty("totalPendingTasks")
    public int totalPendingTasks()
    {
        return totalPendingTasks;
    }
}
