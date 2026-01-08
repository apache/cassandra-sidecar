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
package org.apache.cassandra.sidecar.adapters.base;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.cassandra.sidecar.adapters.base.jmx.CompactionManagerJmxOperations;
import org.apache.cassandra.sidecar.common.server.CompactionManagerOperations;
import org.apache.cassandra.sidecar.common.server.JmxClient;

import static org.apache.cassandra.sidecar.adapters.base.jmx.CompactionManagerJmxOperations.COMPACTION_MANAGER_OBJ_NAME;

/**
 * Default implementation that pulls methods from the Cassandra CompactionManager Proxy
 */
public class CassandraCompactionManagerOperations implements CompactionManagerOperations
{
    private static final List<String> SUPPORTED_COMPACTION_TYPES =
            Arrays.stream(CompactionType.values())
                  .map(CompactionType::name)
                  .collect(Collectors.toList());

    protected final JmxClient jmxClient;

    /**
     * Creates a new instance with the provided {@link JmxClient}
     *
     * @param jmxClient the JMX client used to communicate with the Cassandra instance
     */
    public CassandraCompactionManagerOperations(JmxClient jmxClient)
    {
        this.jmxClient = jmxClient;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Map<String, String>> getCompactions()
    {
        return jmxClient.proxy(CompactionManagerJmxOperations.class, COMPACTION_MANAGER_OBJ_NAME)
                        .getCompactions();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stopCompactionById(String compactionId)
    {
        // compactionId takes precedence over type if both are provided
        if (compactionId != null && !compactionId.trim().isEmpty())
        {
            CompactionManagerJmxOperations proxy = jmxClient.proxy(CompactionManagerJmxOperations.class,
                                                                   COMPACTION_MANAGER_OBJ_NAME);
            proxy.stopCompactionById(compactionId);
        }
        else
        {
            throw new IllegalArgumentException("compaction process with compaction ID "
                                               + compactionId + " is null or empty");
        }
    }

    @Override
    public void stopCompaction(String compactionType)
    {
        if (compactionType != null && !compactionType.trim().isEmpty())
        {
            if (supportedCompactionTypes().contains(compactionType))
            {
                CompactionManagerJmxOperations proxy = jmxClient.proxy(CompactionManagerJmxOperations.class,
                        COMPACTION_MANAGER_OBJ_NAME);
                String errMsg
                        = "compaction process with compaction type " + compactionType + " must not be null when compactionId is not provided";
                proxy.stopCompaction(Objects.requireNonNull(compactionType, errMsg));
            }
            else
            {
                throw new IllegalArgumentException("compaction type " + compactionType + " is not supported");
            }
        }
        else
        {
            throw new IllegalArgumentException("compaction type " + compactionType + " is null or empty");
        }
    }

    @Override
    public List<String> supportedCompactionTypes()
    {
        return SUPPORTED_COMPACTION_TYPES;
    }
}
