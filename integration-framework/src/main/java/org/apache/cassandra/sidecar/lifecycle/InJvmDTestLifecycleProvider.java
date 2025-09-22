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

package org.apache.cassandra.sidecar.lifecycle;

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;

/**
 * Manages the lifecycle of JVM Dtest Cassandra instances.
 * This should be used for integration tests where Cassandra instances are started and stopped
 */
public class InJvmDTestLifecycleProvider implements LifecycleProvider
{
    private final Iterable<? extends IInstance> instances;

    public InJvmDTestLifecycleProvider(Iterable<? extends IInstance> instances)
    {
        this.instances = instances;
    }

    @Override
    public void start(InstanceMetadata instanceMetadata)
    {
        getInstance(instanceMetadata).startup();
    }

    @Override
    public void stop(InstanceMetadata instanceMetadata)
    {
        try
        {
            // Synchronous to ensure JMX port is released before start again
            getInstance(instanceMetadata).shutdown().get(1, TimeUnit.MINUTES);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isRunning(InstanceMetadata host)
    {
        return !getInstance(host).isShutdown();
    }

    private IInstance getInstance(InstanceMetadata instanceMetadata)
    {
        for (IInstance instance : instances)
        {
            if (instance.broadcastAddress().getHostName().equals(instanceMetadata.host()))
            {
                return instance;
            }
        }
        throw new IllegalArgumentException("No instance found for host: " + instanceMetadata);
    }
}
