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

package org.apache.cassandra.sidecar.client;


import java.util.Collections;
import java.util.List;

/**
 * A {@link SidecarInstancesProvider} that returns Sidecar instances from a list of instances.
 */
public class SimpleSidecarInstancesProvider implements SidecarInstancesProvider
{
    private final List<SidecarInstance> sidecarInstances;

    /**
     * Constructs this object with the provided list of Sidecar instances. Creates an unmodifiable copy of the list
     * and keeps a reference to the copy internally.
     *
     * @param sidecarInstances the list of Sidecar instances
     */
    public SimpleSidecarInstancesProvider(List<? extends SidecarInstance> sidecarInstances)
    {
        if (sidecarInstances == null || sidecarInstances.isEmpty())
        {
            String message = "The instances parameter must be non-null and must contain at least one element";
            throw new IllegalArgumentException(message);
        }
        this.sidecarInstances = Collections.unmodifiableList(sidecarInstances);
    }

    @Override
    public List<SidecarInstance> instances()
    {
        return sidecarInstances;
    }
}
