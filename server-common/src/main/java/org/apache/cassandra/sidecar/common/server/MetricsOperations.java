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

import java.util.Map;

import org.apache.cassandra.sidecar.common.response.ClientStatsResponse;

/**
 * An interface that defines interactions with the metrics system in Cassandra.
 */
public interface MetricsOperations
{
    /**
     * Retrieve the connected client stats metrics from the cluster
     * @param isListConnections boolean parameter to list connection details
     * @param isVerbose boolean parameter for verbose response
     * @param isByProtocol boolean parameter to return stats by protocol-version
     * @param isClientOptions boolean parameter to include client-options
     * @return the requested client stats
     */
    ClientStatsResponse clientStats(boolean isListConnections, boolean isVerbose, boolean isByProtocol, boolean isClientOptions);

}
