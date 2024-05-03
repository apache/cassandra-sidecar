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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.sidecar.common.server.JmxClient;
import org.apache.cassandra.sidecar.common.server.TableOperations;
import org.jetbrains.annotations.NotNull;

/**
 * An implementation of the {@link TableOperations} that interfaces with Cassandra 4.0 and later
 */
public class CassandraTableOperations implements TableOperations
{
    private final JmxClient jmxClient;

    public CassandraTableOperations(JmxClient jmxClient)
    {
        this.jmxClient = jmxClient;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> importNewSSTables(@NotNull String keyspace,
                                          @NotNull String tableName,
                                          @NotNull String directory,
                                          boolean resetLevel,
                                          boolean clearRepaired,
                                          boolean verifySSTables,
                                          boolean verifyTokens,
                                          boolean invalidateCaches,
                                          boolean extendedVerify,
                                          boolean copyData)
    {
        return jmxClient.proxy(TableJmxOperations.class, tableMBeanName(keyspace, tableName))
                        .importNewSSTables(Collections.singleton(directory),
                                           resetLevel,
                                           clearRepaired,
                                           verifySSTables,
                                           verifyTokens,
                                           invalidateCaches,
                                           extendedVerify,
                                           copyData);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getDataPaths(@NotNull String keyspace, @NotNull String table) throws IOException
    {
        return jmxClient.proxy(TableJmxOperations.class, tableMBeanName(keyspace, table))
                        .getDataPaths();
    }

    String tableMBeanName(String keyspace, String tableName)
    {
        return String.format("org.apache.cassandra.db:type=%s,keyspace=%s,table=%s",
                             tableName.contains(".") ? "IndexTables" : "Tables",
                             keyspace, tableName);
    }
}
