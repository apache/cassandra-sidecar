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

package org.apache.cassandra.sidecar.cdc;

import java.util.Comparator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.KeyspaceMetadata;
import org.apache.cassandra.cdc.sidecar.ReplicationFactorSupplier;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.spark.data.ReplicationFactor;

/**
 * Resolves a keyspace's actual {@link ReplicationFactor} from live Cassandra schema metadata
 * via the driver. This replaces the analytics library's {@code ReplicationFactorSupplier.DEFAULT},
 * which hard-codes {@code SimpleStrategy/RF=1} and would otherwise cause CDC peer discovery
 * to find only one replica per token range.
 */
public class SidecarReplicationFactorSupplier implements ReplicationFactorSupplier
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarReplicationFactorSupplier.class);

    private final InstanceMetadataFetcher instanceMetadataFetcher;

    public SidecarReplicationFactorSupplier(InstanceMetadataFetcher instanceMetadataFetcher)
    {
        this.instanceMetadataFetcher = instanceMetadataFetcher;
    }

    @Override
    public ReplicationFactor getReplicationFactor(String keyspace)
    {
        return instanceMetadataFetcher.callOnFirstAvailableInstance(instance -> {
            KeyspaceMetadata ks = instance.delegate().metadata().getKeyspace(keyspace);
            if (ks == null)
            {
                LOGGER.warn("Keyspace '{}' not found in driver metadata, falling back to SimpleStrategy/RF=1", keyspace);
                return ReplicationFactor.simpleStrategy(1);
            }
            ReplicationFactor rf = new ReplicationFactor(ks.getReplication());
            LOGGER.info("Resolved replicationFactor for keyspace={}: strategy={} totalRf={}",
                         keyspace, rf.getReplicationStrategy(), rf.getTotalReplicationFactor());
            return rf;
        });
    }

    @Override
    public ReplicationFactor getMaximalReplicationFactor()
    {
        // The analytics layer asks for the "maximal" RF across CDC-enabled keyspaces to size
        // partition assignment; we approximate by taking the schema-wide max-total-RF keyspace.
        // Falls back to SimpleStrategy/RF=1 if no keyspaces are visible (extreme edge case).
        return instanceMetadataFetcher.callOnFirstAvailableInstance(instance -> {
            KeyspaceMetadata maxKs = instance.delegate().metadata().getKeyspaces().stream()
                                              .max(Comparator.comparingInt(SidecarReplicationFactorSupplier::totalRf))
                                              .orElse(null);
            if (maxKs == null)
            {
                LOGGER.warn("No keyspaces visible from driver; falling back to SimpleStrategy/RF=1");
                return ReplicationFactor.simpleStrategy(1);
            }
            ReplicationFactor rf = new ReplicationFactor(maxKs.getReplication());
            LOGGER.info("Resolved maximalReplicationFactor from keyspace={} totalRf={}",
                         maxKs.getName(), rf.getTotalReplicationFactor());
            return rf;
        });
    }

    private static int totalRf(KeyspaceMetadata ks)
    {
        int total = 0;
        for (Map.Entry<String, String> e : ks.getReplication().entrySet())
        {
            // Skip the "class" entry; only DC/replication-factor entries are integer-valued.
            if ("class".equals(e.getKey()))
            {
                continue;
            }
            try
            {
                total += Integer.parseInt(e.getValue());
            }
            catch (NumberFormatException ignored)
            {
                LOGGER.warn("Non-integer replication option: {}", e.getValue());
            }
        }
        return total;
    }
}
