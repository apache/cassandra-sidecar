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

package org.apache.cassandra.sidecar.common.server.cluster.locator;

/**
 * A simplified mirror of {@code IPartitioner} in Cassandra
 */
public interface Partitioner
{
    /**
     * @return the minimum token of the partitioner
     *
     * Note that the minimum token is not assigned to any data, i.e. excluded from the token ring
     */
    Token minimumToken();

    /**
     * @return the maximum token of the partitioner
     *
     * Note that the maximum token is included in the token ring
     */
    Token maximumToken();

    /**
     * @return name of the Partitioner
     */
    default String name()
    {
        return this.getClass().getSimpleName();
    }
}
