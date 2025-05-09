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

package org.apache.cassandra.sidecar.client.selection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.client.SidecarInstancesProvider;
import org.jetbrains.annotations.NotNull;

/**
 * An instance selection policy that returns instances in random order, and does not necessarily return the instances
 * in the provided order from the underlying {@link Iterator} for the {@link SidecarInstancesProvider#instances()}.
 */
public class RandomInstanceSelectionPolicy extends OrderedInstanceSelectionPolicy
{
    /**
     * Constructs a new {@link RandomInstanceSelectionPolicy} with the provided list of instances
     *
     * @param instancesProvider the class that provides the sidecar instances
     */
    public RandomInstanceSelectionPolicy(SidecarInstancesProvider instancesProvider)
    {
        super(instancesProvider);
    }

    /**
     * Returns an iterator of {@link SidecarInstance instances} in random order. This is a non-deterministic operation
     * because the underlying list will be shuffled every time this iterator is called.
     *
     * @return an iterator of {@link SidecarInstance instances} in random order
     */
    @Override
    @NotNull
    public Iterator<SidecarInstance> iterator()
    {
        List<SidecarInstance> instances = new ArrayList<>(instancesProvider.instances());
        Collections.shuffle(instances, ThreadLocalRandom.current());
        return instances.iterator();
    }
}
