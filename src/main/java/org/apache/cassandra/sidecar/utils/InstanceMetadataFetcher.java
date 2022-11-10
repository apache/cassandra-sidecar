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

package org.apache.cassandra.sidecar.utils;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.CassandraAdapterDelegate;

import static org.apache.cassandra.sidecar.utils.RequestUtils.extractHostAddressWithoutPort;

/**
 * To fetch instance information according to instance id provided.
 * By default returns 1st instance's information
 */
@Singleton
public class InstanceMetadataFetcher
{
    private final InstancesConfig instancesConfig;

    @Inject
    public InstanceMetadataFetcher(InstancesConfig instancesConfig)
    {
        this.instancesConfig = instancesConfig;
    }

    public CassandraAdapterDelegate getDelegate(String host)
    {
        return host == null
               ? getFirstInstance().delegate()
               : instancesConfig.instanceFromHost(host).delegate();
    }

    public CassandraAdapterDelegate getDelegate(Integer instanceId)
    {
        return instanceId == null
               ? getFirstInstance().delegate()
               : instancesConfig.instanceFromId(instanceId).delegate();
    }

    public CassandraAdapterDelegate getDelegate(String host, Integer instanceId)
    {
        CassandraAdapterDelegate cassandra;
        if (instanceId != null)
        {
            cassandra = getDelegate(instanceId);
        }
        else
        {
            cassandra = getDelegate(extractHostAddressWithoutPort(host));
        }
        return cassandra;
    }

    private InstanceMetadata getFirstInstance()
    {
        if (instancesConfig.instances().isEmpty())
        {
            throw new IllegalStateException("There are no instances configured!");
        }
        return instancesConfig.instances().get(0);
    }
}
