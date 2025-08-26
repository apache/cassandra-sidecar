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

import java.util.List;
import java.util.Map;

import org.apache.cassandra.sidecar.adapters.base.jmx.CompactionManagerJmxOperations;
import org.apache.cassandra.sidecar.common.server.CompactionManagerOperations;
import org.apache.cassandra.sidecar.common.server.JmxClient;

import static org.apache.cassandra.sidecar.adapters.base.jmx.CompactionManagerJmxOperations.COMPACTION_MANAGER_OBJ_NAME;

/**
 * Default implementation that pulls methods from the Cassandra CompactionManager Proxy
 */
public class CassandraCompactionManagerOperations implements CompactionManagerOperations
{
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
}
