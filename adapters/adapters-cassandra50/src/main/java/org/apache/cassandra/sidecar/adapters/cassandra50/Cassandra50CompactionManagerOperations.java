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

package org.apache.cassandra.sidecar.adapters.cassandra50;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.sidecar.adapters.base.CassandraCompactionManagerOperations;
import org.apache.cassandra.sidecar.common.server.JmxClient;

/**
 * Cassandra 5.0 implementation of CompactionManagerOperations that supports MAJOR_COMPACTION
 * which is only available in Cassandra 5.x versions
 */
public class Cassandra50CompactionManagerOperations extends CassandraCompactionManagerOperations
{
    private static final List<String> SUPPORTED_COMPACTION_TYPES =
            Arrays.stream(org.apache.cassandra.sidecar.adapters.cassandra50.CompactionType.values())
                    .map(org.apache.cassandra.sidecar.adapters.cassandra50.CompactionType::name)
                    .collect(Collectors.toList());
    /**
     * Creates a new instance with the provided {@link JmxClient}
     *
     * @param jmxClient the JMX client used to communicate with the Cassandra instance
     */
    public Cassandra50CompactionManagerOperations(JmxClient jmxClient)
    {
        super(jmxClient);
    }

    /**
     * {@inheritDoc}
     *
     * Cassandra 5.x supports MAJOR_COMPACTION operation type
     */
    @Override
    public List<String> supportedCompactionTypes()
    {
        return SUPPORTED_COMPACTION_TYPES;
    }
}
