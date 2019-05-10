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
package org.apache.cassandra.sidecar;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import javax.annotation.Nullable;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.NotImplementedException;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.mapping.DefaultNamingStrategy;
import com.datastax.driver.mapping.DefaultPropertyMapper;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingConfiguration;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.NamingConventions;
import com.datastax.driver.mapping.PropertyAccessStrategy;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Table;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Exposee virtual tables contents as pojos
 */
@Singleton
public class VirtualTables
{
    private final CQLSession session;
    private MappingConfiguration conf;

    private MappingManager mappingManager;

    @Inject
    public VirtualTables(CQLSession session)
    {
        this.session = session;
        conf = MappingConfiguration.builder()
                            .withPropertyMapper(
                                new DefaultPropertyMapper()
                                    .setNamingStrategy(
                                        new DefaultNamingStrategy(
                                            NamingConventions.LOWER_CAMEL_CASE, NamingConventions.LOWER_SNAKE_CASE))
                                    .setPropertyAccessStrategy(PropertyAccessStrategy.FIELDS))
                            .build();
    }

    @Nullable
    MappingManager manger()
    {
        if (mappingManager != null)
            return mappingManager;
        if (session.getLocalCql() == null)
            return null;

        // its not ideal but ok if we create multiple during race
        mappingManager = new MappingManager(session.getLocalCql(), conf);
        return mappingManager;
    }

    public <T> ListenableFuture<Result<T>> getTableResults(Class<T> pojo)
    {
        Table[] annotation = pojo.getAnnotationsByType(Table.class);
        if (annotation.length != 1 || !annotation[0].keyspace().equals("system_views"))
            throw new IllegalArgumentException("Invalid class type");

        if (manger() == null)
            throw new NoHostAvailableException(Collections.emptyMap());

        Session local = session.getLocalCql();
        assert local != null; // manager() null check should guard against this

        Collection<Host> hosts = local.getState().getConnectedHosts();
        if (hosts.isEmpty())
            throw new NoHostAvailableException(Collections.emptyMap());
        if (hosts.iterator().next().getCassandraVersion().getMajor() < 4)
            throw new NotImplementedException("Requires version 4.0+");

        String table = annotation[0].name();
        Mapper<T> mapper = mappingManager.mapper(pojo);

        ResultSetFuture results = local.executeAsync("SELECT * FROM system_views." + table);
        return mapper.mapAsync(results);
    }

    /**
     * Represents state of a thread pool from the thread_pools virtual table
     */
    @Table(keyspace = "system_views", name = "thread_pools")
    public static class ThreadStats
    {
        private String name;
        private int activeTasks;
        private int activeTasksLimit;
        private int pendingTasks;
        private long completedTasks;
        private long blockedTasks;
        private long blockedTasksAllTime;

        public String getName()
        {
            return name;
        }

        public int getActiveTasks()
        {
            return activeTasks;
        }

        public int getActiveTasksLimit()
        {
            return activeTasksLimit;
        }

        public int getPendingTasks()
        {
            return pendingTasks;
        }

        public long getCompletedTasks()
        {
            return completedTasks;
        }

        public long getBlockedTasks()
        {
            return blockedTasks;
        }

        public long getBlockedTasksAllTime()
        {
            return blockedTasksAllTime;
        }
    }

    public ListenableFuture<Result<ThreadStats>> threadStats()
    {
        return getTableResults(ThreadStats.class);
    }

    /**
     * Represents state of a tasks from the sstable_tasks virtual table
     */
    @Table(keyspace = "system_views", name = "sstable_tasks")
    public static class SSTableTask
    {
        private String keyspaceName;
        private String tableName;
        private UUID taskId;
        private String kind;
        private long progress;
        private long total;
        private String unit;

        public String getKeyspaceName()
        {
            return keyspaceName;
        }

        public String getTableName()
        {
            return tableName;
        }

        public UUID getTaskId()
        {
            return taskId;
        }

        public String getKind()
        {
            return kind;
        }

        public long getProgress()
        {
            return progress;
        }

        public long getTotal()
        {
            return total;
        }

        public String getUnit()
        {
            return unit;
        }
    }

    public ListenableFuture<Result<SSTableTask>> sstableTasks()
    {
        return getTableResults(SSTableTask.class);
    }

    /**
     * Represents state of a setting from the settings virtual table
     */
    @Table(keyspace = "system_views", name = "settings")
    public static class Setting
    {
        private String name;
        private String value;

        public String getValue()
        {
            return value;
        }

        public String getName()
        {
            return name;
        }
    }

    public ListenableFuture<Result<Setting>> settings()
    {
        return getTableResults(Setting.class);
    }

    /**
     * Represents state of a client from the clients virtual table
     */
    @Table(keyspace = "system_views", name = "clients")
    public static class Client
    {
        private InetAddress address;
        private int port;
        private String hostname;
        private String username;
        private String connectionStage;
        private int protocolVersion;
        private String driverName;
        private String driverVersion;
        private long requestCount;
        private boolean sslEnabled;
        private String sslProtocol;
        private String sslCipherSuite;

        public InetAddress getAddress()
        {
            return address;
        }

        public int getPort()
        {
            return port;
        }

        public String getHostname()
        {
            return hostname;
        }

        public String getUsername()
        {
            return username;
        }

        public String getConnectionStage()
        {
            return connectionStage;
        }

        public int getProtocolVersion()
        {
            return protocolVersion;
        }

        public String getDriverName()
        {
            return driverName;
        }

        public String getDriverVersion()
        {
            return driverVersion;
        }

        public long getRequestCount()
        {
            return requestCount;
        }

        public boolean isSslEnabled()
        {
            return sslEnabled;
        }

        public String getSslProtocol()
        {
            return sslProtocol;
        }

        public String getSslCipherSuite()
        {
            return sslCipherSuite;
        }
    }

    public ListenableFuture<Result<Client>> clients()
    {
        return getTableResults(Client.class);
    }
}
