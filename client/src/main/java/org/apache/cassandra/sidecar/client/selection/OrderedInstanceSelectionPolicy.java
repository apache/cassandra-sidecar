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

import java.util.Iterator;

import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.apache.cassandra.sidecar.client.SidecarInstancesProvider;
import org.jetbrains.annotations.NotNull;

/**
 * A selection policy for a multiple Cassandra Sidecar instances where the iterator returns in order
 */
public class OrderedInstanceSelectionPolicy implements InstanceSelectionPolicy
{
    protected final SidecarInstancesProvider instancesProvider;

    /**
     * Constructs a new {@link OrderedInstanceSelectionPolicy} with the provided list of instances
     *
     * @param instancesProvider the class that provides the sidecar instances
     */
    public OrderedInstanceSelectionPolicy(SidecarInstancesProvider instancesProvider)
    {
        this.instancesProvider = instancesProvider;
    }

    /**
     * Returns an iterator of {@link SidecarInstance instances}.
     *
     * @return an iterator of {@link SidecarInstance instances}
     */
    @NotNull
    public Iterator<SidecarInstance> iterator()
    {
        return instancesProvider.instances().iterator();
    }
}
