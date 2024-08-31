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

package org.apache.cassandra.sidecar.cluster.locator;

import java.util.Map;
import java.util.Set;

import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;

/**
 * Provides the token ranges of the local Cassandra instance(s)
 */
public interface LocalTokenRangesProvider
{
    /**
     * Calculate the token ranges owned and replicated to the local Cassandra instance(s).
     * When Sidecar is paired with multiple Cassandra instance, the ranges of each Cassandra instance is captured
     * in the form of map, where the key is the instance id and the value is the ranges of the Cassandra instance.
     * When Cassandra is not running with VNode, the set of ranges has a single value.
     * When Sidecar is paired with a single Cassandra instance, the result map has a single entry.
     *
     * @param keyspace keyspace to determine replication
     * @return token ranges of the local Cassandra instances or an empty map of nothing is found
     */
    Map<Integer, Set<TokenRange>> localTokenRanges(String keyspace);
}
