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
import java.util.NoSuchElementException;

import org.apache.cassandra.sidecar.client.SidecarInstance;
import org.jetbrains.annotations.NotNull;

/**
 * A selection policy for a single Cassandra Sidecar instance
 */
public class SingleInstanceSelectionPolicy implements InstanceSelectionPolicy
{
    private final SidecarInstance sidecarInstance;

    /**
     * Constructs a {@link SingleInstanceSelectionPolicy} with the provided {@code sidecarInstance}.
     *
     * @param sidecarInstance the sidecar instance
     */
    public SingleInstanceSelectionPolicy(SidecarInstance sidecarInstance)
    {
        this.sidecarInstance = sidecarInstance;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NotNull
    public Iterator<SidecarInstance> iterator()
    {
        return new Iterator<SidecarInstance>()
        {
            boolean hasNext = true;

            @Override
            public boolean hasNext()
            {
                return hasNext;
            }

            @Override
            public SidecarInstance next()
            {
                if (!hasNext)
                {
                    throw new NoSuchElementException();
                }
                hasNext = false;
                return sidecarInstance;
            }
        };
    }
}
