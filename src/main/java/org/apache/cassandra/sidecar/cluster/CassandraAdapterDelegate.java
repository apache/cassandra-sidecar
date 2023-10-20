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

package org.apache.cassandra.sidecar.cluster;

import java.io.IOException;
import java.util.Objects;
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
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.cassandra.sidecar.common.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.ClusterMembershipOperations;
import org.apache.cassandra.sidecar.common.ICassandraAdapter;
import org.apache.cassandra.sidecar.common.JmxClient;
import org.apache.cassandra.sidecar.common.NodeSettings;
import org.apache.cassandra.sidecar.common.StorageOperations;
import org.apache.cassandra.sidecar.common.TableOperations;
import org.apache.cassandra.sidecar.utils.CassandraVersionProvider;
import org.apache.cassandra.sidecar.utils.SimpleCassandraVersion;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CASSANDRA_CQL_DISCONNECTED;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CASSANDRA_CQL_READY;


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

    private final Vertx vertx;
    private final int cassandraInstanceId;
    private final String sidecarVersion;
    private final CassandraVersionProvider versionProvider;
    private final CQLSessionProvider cqlSessionProvider;
    private final JmxClient jmxClient;
    private SimpleCassandraVersion currentVersion;
    private ICassandraAdapter adapter;
    private volatile NodeSettings nodeSettings = null;
    private final AtomicBoolean registered = new AtomicBoolean(false);
    private final AtomicBoolean isHealthCheckActive = new AtomicBoolean(false);

    /**
     * Constructs a new {@link CassandraAdapterDelegate} for the given {@code cassandraInstance}
     *
     * @param vertx               the vertx instance
     * @param cassandraInstanceId the cassandra instance identifier
     * @param versionProvider     a Cassandra version provider
     * @param session             the session to the Cassandra database
     * @param jmxClient           the JMX client used to communicate with the Cassandra instance
     * @param sidecarVersion      the version of the Sidecar from the current binary
     */
    public CassandraAdapterDelegate(Vertx vertx,
                                    int cassandraInstanceId,
                                    CassandraVersionProvider versionProvider,
                                    CQLSessionProvider session,
                                    JmxClient jmxClient,
                                    String sidecarVersion)
    {
        this.vertx = Objects.requireNonNull(vertx);
        this.cassandraInstanceId = cassandraInstanceId;
        this.sidecarVersion = sidecarVersion;
        this.versionProvider = versionProvider;
        this.cqlSessionProvider = session;
        this.jmxClient = jmxClient;
    }

    private void maybeRegisterHostListener(@NotNull Session session)
    {
        if (!registered.get())
        {
            Cluster cluster = session.getCluster();
            if (!cluster.isClosed() && registered.compareAndSet(false, true))
            {
                cluster.register(this);
            }
        }
    }

    private void maybeUnregisterHostListener(@NotNull Session session)
    {
        if (registered.get())
        {
            Cluster cluster = session.getCluster();
            if (!cluster.isClosed() && registered.compareAndSet(true, false))
            {
                cluster.unregister(this);
            }
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
            LOGGER.debug("Skipping health check for cassandraInstanceId={} because there's " +
                         "an active check at the moment", cassandraInstanceId);
        }
    }

    private void healthCheckInternal()
    {
        Session activeSession = cqlSessionProvider.localCql();
        if (activeSession == null)
        {
            LOGGER.info("No local CQL session is available for cassandraInstanceId={}. Cassandra is down presumably.",
                        cassandraInstanceId);
            markAsDownAndMaybeNotify();
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
                LOGGER.info("Cassandra version change detected (from={} to={}) for cassandraInstanceId={}. " +
                            "New adapter loaded={}", previousVersion, currentVersion, cassandraInstanceId, adapter);

                notifyCqlConnection();
            }
            LOGGER.debug("Cassandra version {}", releaseVersion);
        }
        catch (IllegalArgumentException | NoHostAvailableException e)
        {
            LOGGER.error("Unexpected error connecting to Cassandra instance {}", cassandraInstanceId, e);
            // The cassandra node is down.
            // Unregister the host listener and nullify the session in order to get a new object.
            markAsDownAndMaybeNotify();
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
        markAsDownAndMaybeNotify();
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
        markAsDownAndMaybeNotify();
        Session activeSession = cqlSessionProvider.close();
        if (activeSession != null)
        {
            maybeUnregisterHostListener(activeSession);
        }
        if (jmxClient != null)
        {
            try
            {
                jmxClient.close();
            }
            catch (IOException e)
            {
                LOGGER.warn("Unable to close JMX client", e);
            }
        }
    }

    public SimpleCassandraVersion version()
    {
        healthCheck();
        return currentVersion;
    }

    protected void notifyCqlConnection()
    {
        JsonObject connectMessage = new JsonObject()
                                    .put("cassandraInstanceId", cassandraInstanceId);
        vertx.eventBus().publish(ON_CASSANDRA_CQL_READY.address(), connectMessage);
        LOGGER.info("CQL connected to cassandraInstanceId={}", cassandraInstanceId);
    }

    protected void markAsDownAndMaybeNotify()
    {
        NodeSettings currentNodeSettings = nodeSettings;
        nodeSettings = null;
        if (currentNodeSettings != null)
        {
            JsonObject disconnectMessage = new JsonObject()
                                           .put("cassandraInstanceId", cassandraInstanceId);
            vertx.eventBus().publish(ON_CASSANDRA_CQL_DISCONNECTED.address(), disconnectMessage);
            LOGGER.info("CQL disconnection from cassandraInstanceId={}", cassandraInstanceId);
        }
    }

    @Nullable
    private <T> T fromAdapter(Function<ICassandraAdapter, T> getter)
    {
        ICassandraAdapter localAdapter = this.adapter;
        return localAdapter == null ? null : getter.apply(localAdapter);
    }
}
