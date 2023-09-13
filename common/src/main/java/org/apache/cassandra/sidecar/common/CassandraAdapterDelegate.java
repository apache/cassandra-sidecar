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

package org.apache.cassandra.sidecar.common;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


/**
 * Since it's possible for the version of Cassandra to change under us, we need this delegate to wrap the functionality
 * of the underlying Cassandra adapter.  If a server reboots, we can swap out the right Adapter when the driver
 * reconnects.
 *
 * <ol>
 * <li>The session lazily connects</li>
 * <li>We might need to swap out the adapter if the version has changed</li>
 * </ol>
 */
public class CassandraAdapterDelegate implements ICassandraAdapter, Host.StateListener
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraAdapterDelegate.class);

    private final String sidecarVersion;
    private final CassandraVersionProvider versionProvider;
    private final CQLSessionProvider cqlSessionProvider;
    private final JmxClient jmxClient;
    private SimpleCassandraVersion currentVersion;
    private ICassandraAdapter adapter;
    private volatile NodeSettings nodeSettings = null;
    private final AtomicBoolean registered = new AtomicBoolean(false);
    private final AtomicBoolean isHealthCheckActive = new AtomicBoolean(false);

    public CassandraAdapterDelegate(CassandraVersionProvider versionProvider,
                                    CQLSessionProvider cqlSessionProvider,
                                    JmxClient jmxClient,
                                    String sidecarVersion)
    {
        this.sidecarVersion = sidecarVersion;
        this.versionProvider = versionProvider;
        this.cqlSessionProvider = cqlSessionProvider;
        this.jmxClient = jmxClient;
    }

    private void maybeRegisterHostListener(@NotNull Session session)
    {
        if (registered.compareAndSet(false, true))
        {
            session.getCluster().register(this);
        }
    }

    private void maybeUnregisterHostListener(@NotNull Session session)
    {
        if (registered.compareAndSet(true, false))
        {
            session.getCluster().unregister(this);
        }
    }

    /**
     * Should be called on initial connect as well as when a server comes back since it might be from an upgrade
     * synchronized so we don't flood the DB with version requests
     *
     * <p>If the healthcheck determines we've changed versions, it should load the proper adapter</p>
     */
    public void healthCheck()
    {
        if (isHealthCheckActive.compareAndSet(false, true))
        {
            try
            {
                healthCheckInternal();
            }
            finally
            {
                isHealthCheckActive.set(false);
            }
        }
        else
        {
            LOGGER.debug("Skipping health check because there's an active check at the moment");
        }
    }

    private void healthCheckInternal()
    {
        Session activeSession = cqlSessionProvider.localCql();
        if (activeSession == null)
        {
            LOGGER.info("No local CQL session is available. Cassandra is down presumably.");
            nodeSettings = null;
            return;
        }

        maybeRegisterHostListener(activeSession);

        try
        {
            Row oneResult = activeSession.execute("select release_version, partitioner from system.local")
                                         .one();

            // Note that within the scope of this method, we should keep on using the local releaseVersion
            String releaseVersion = oneResult.getString("release_version");
            NodeSettings newNodeSettings = new NodeSettings(releaseVersion,
                                                            oneResult.getString("partitioner"),
                                                            sidecarVersion);
            if (!newNodeSettings.equals(nodeSettings))
            {
                // Update the nodeSettings cache
                SimpleCassandraVersion previousVersion = currentVersion;
                currentVersion = SimpleCassandraVersion.create(releaseVersion);
                adapter = versionProvider.cassandra(releaseVersion)
                                         .create(cqlSessionProvider, jmxClient);
                nodeSettings = newNodeSettings;
                LOGGER.info("Cassandra version change detected (from={} to={}). New adapter loaded={}",
                            previousVersion, currentVersion, adapter);
            }
            LOGGER.debug("Cassandra version {}", releaseVersion);
        }
        catch (IllegalArgumentException | NoHostAvailableException e)
        {
            LOGGER.error("Unexpected error connecting to Cassandra instance.", e);
            // The cassandra node is down.
            // Unregister the host listener and nullify the session in order to get a new object.
            nodeSettings = null;
            maybeUnregisterHostListener(activeSession);
            cqlSessionProvider.close();
        }
    }

    /**
     * @return metadata on the connected cluster, including known nodes and schema definitions obtained from the
     * {@link ICassandraAdapter}
     */
    @Nullable
    @Override
    public Metadata metadata()
    {
        return fromAdapter(ICassandraAdapter::metadata);
    }

    /**
     * @return a cached {@link NodeSettings}. The returned value could be null when no CQL connection is established
     */
    @Nullable
    @Override
    public NodeSettings nodeSettings()
    {
        return nodeSettings;
    }

    @Nullable
    @Override
    public StorageOperations storageOperations()
    {
        return fromAdapter(ICassandraAdapter::storageOperations);
    }

    @Nullable
    @Override
    public ClusterMembershipOperations clusterMembershipOperations()
    {
        return fromAdapter(ICassandraAdapter::clusterMembershipOperations);
    }

    @Nullable
    @Override
    public TableOperations tableOperations()
    {
        return fromAdapter(ICassandraAdapter::tableOperations);
    }

    @Override
    public void onAdd(Host host)
    {
        healthCheck();
    }

    @Override
    public void onUp(Host host)
    {
        healthCheck();
    }

    @Override
    public void onDown(Host host)
    {
        nodeSettings = null;
    }

    @Override
    public void onRemove(Host host)
    {
        healthCheck();
    }

    @Override
    public void onRegister(Cluster cluster)
    {
    }

    @Override
    public void onUnregister(Cluster cluster)
    {
    }

    public boolean isUp()
    {
        return nodeSettings != null;
    }

    public void close()
    {
        nodeSettings = null;
        Session activeSession = cqlSessionProvider.close();
        if (activeSession != null)
        {
            maybeUnregisterHostListener(activeSession);
        }
        try
        {
            jmxClient.close();
        }
        catch (IOException e)
        {
            LOGGER.warn("Unable to close JMX client", e);
        }
    }

    public SimpleCassandraVersion version()
    {
        healthCheck();
        return currentVersion;
    }

    @Nullable
    private <T> T fromAdapter(Function<ICassandraAdapter, T> getter)
    {
        ICassandraAdapter localAdapter = this.adapter;
        return localAdapter == null ? null : getter.apply(localAdapter);
    }
}
