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

import com.datastax.driver.core.Metadata;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.TableOperations;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;
import org.apache.cassandra.sidecar.metrics.instance.InstanceHealthMetrics;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException.Service.CQL_AND_JMX;
import static org.apache.cassandra.sidecar.utils.TestMetricUtils.registry;

/**
 * A fake delegate used for testing
 */
public class TestCassandraAdapterDelegate extends CassandraAdapterDelegate
{
    Metadata metadata;
    StorageOperations storageOperations;
    TableOperations tableOperations;
    NodeSettings nodeSettings;
    boolean isNativeUp = false;

    public TestCassandraAdapterDelegate(Vertx vertx)
    {
        super(vertx, 1, null, null, null, null, null, "localhost", 9042, new InstanceHealthMetrics(registry(1)));
    }

    @Override
    protected CassandraAdapterDelegate.JmxNotificationListener initializeJmxListener()
    {
        return null;
    }

    @Override
    public void healthCheck()
    {
        // do nothing
    }

    @Override
    @NotNull
    public Metadata metadata()
    {
        return throwOnNull(metadata);
    }

    public void setMetadata(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    @NotNull
    public TableOperations tableOperations()
    {
        return throwOnNull(tableOperations);
    }

    public void setTableOperations(TableOperations tableOperations)
    {
        this.tableOperations = tableOperations;
    }

    @Override
    @NotNull
    public NodeSettings nodeSettings()
    {
        return throwOnNull(nodeSettings);
    }

    public void setNodeSettings(NodeSettings nodeSettings)
    {
        this.nodeSettings = nodeSettings;
    }

    @Override
    public boolean isNativeUp()
    {
        return isNativeUp;
    }

    public void setIsNativeUp(boolean isUp)
    {
        this.isNativeUp = isUp;
    }

    @Override
    @NotNull
    public StorageOperations storageOperations()
    {
        return throwOnNull(storageOperations);
    }

    public void setStorageOperations(StorageOperations storageOperations)
    {
        this.storageOperations = storageOperations;
    }

    @Override
    public void close()
    {
        // no resource to close
    }

    private <T> T throwOnNull(T value)
    {
        if (value == null)
        {
            throw new CassandraUnavailableException(CQL_AND_JMX, "Cassandra unavailable");
        }
        return value;
    }
}
