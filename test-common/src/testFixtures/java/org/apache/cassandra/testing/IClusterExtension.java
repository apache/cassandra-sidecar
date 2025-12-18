/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.testing;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;

/**
 * Extends functionality for the {@link ICluster} interface
 *
 * @param <I> the type of the cluster instances
 */
public interface IClusterExtension<I extends IInstance> extends ICluster<I>
{
    /**
     * Create a new instance and add it to the cluster, without starting it.
     *
     * @param dc   the instance should be in
     * @param rack the instance should be in
     * @param fn   function to add to the config before starting
     * @return the instance added
     */
    I addInstance(String dc, String rack, Consumer<IInstanceConfig> fn);

    /**
     * @return a reference to the delegated {@link ICluster} instance
     */
    ICluster<I> delegate();

    /**
     * Waits for the ring to have the target instance with the provided state.
     *
     * @param instance       instance to check on
     * @param expectedInRing to look for
     * @param state          expected
     */
    void awaitRingState(IInstance instance, IInstance expectedInRing, String state);

    /**
     * Wait for the ring to have the target instance with the provided status.
     *
     * @param instance       instance to check on
     * @param expectedInRing to look for
     * @param status         expected
     */
    void awaitRingStatus(IInstance instance, IInstance expectedInRing, String status);

    /**
     * Waits for the target instance to have the desired status. Target status is checked via string contains so works
     * with 'NORMAL' but also can check tokens or full state.
     *
     * @param instance         instance to check on
     * @param expectedInGossip instance to wait for
     * @param targetStatus     for the instance
     */
    void awaitGossipStatus(IInstance instance, IInstance expectedInGossip, String targetStatus);

    /**
     * Stop an instance in a blocking manner.
     * <p>The main difference between this and {@link IInstance#shutdown()} is that the wait on the future will catch
     * the exceptions and throw as runtime.
     *
     * @param instance instance to stop
     */
    void stopUnchecked(IInstance instance);

    /**
     * @param instance the instance where ring will be executed
     * @return a list of {@link IRingEntry} that represent the ring view of the provided {@code instance}
     */
    List<IRingEntry> ring(IInstance instance);

    /**
     * Start the instance with the given system properties; after the instance has started,
     * the properties will be cleared.
     *
     * @param instance         the instance to start
     * @param systemProperties the system properties used during startup of the instance
     */
    IInstance start(IInstance instance, Map<String, String> systemProperties);
}
