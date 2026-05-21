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

package org.apache.cassandra.sidecar.cdc;

import java.util.Map;

import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.cdc.api.CdcOptions;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.spark.data.ReplicationFactor;

/**
 * Specific sidecar CDC options
 */
public class SidecarCdcOptions implements CdcOptions
{

    private final InstanceMetadataFetcher instanceMetadataFetcher;

    public SidecarCdcOptions(InstanceMetadataFetcher instanceMetadataFetcher)
    {
        this.instanceMetadataFetcher = instanceMetadataFetcher;
    }


    public ReplicationFactor replicationFactor(String keyspace)
    {

        Map<String, String> replication = instanceMetadataFetcher
                                          .callOnFirstAvailableInstance(instance-> instance.delegate().metadata().getKeyspace(keyspace).getReplication());
        return new ReplicationFactor(replication);
    }

    public String dc()
    {
        return instanceMetadataFetcher.callOnFirstAvailableInstance(instance-> instance.delegate().nodeSettings().datacenter());
    }

    @Override
    public CassandraVersion version()
    {
        String releaseVersion = instanceMetadataFetcher.callOnFirstAvailableInstance(
                instance -> instance.delegate().nodeSettings().releaseVersion());
        return CassandraVersion.fromVersion(releaseVersion).orElse(CassandraVersion.FOURZERO);
    }
}
