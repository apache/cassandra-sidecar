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
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DriverUtils;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
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

import static org.apache.cassandra.sidecar.common.NodeSettings.DATA_CENTER_COLUMN_NAME;
import static org.apache.cassandra.sidecar.common.NodeSettings.PARTITIONER_COLUMN_NAME;
import static org.apache.cassandra.sidecar.common.NodeSettings.RELEASE_VERSION_COLUMN_NAME;
import static org.apache.cassandra.sidecar.common.NodeSettings.RPC_ADDRESS_COLUMN_NAME;
import static org.apache.cassandra.sidecar.common.NodeSettings.RPC_PORT_COLUMN_NAME;
import static org.apache.cassandra.sidecar.common.NodeSettings.TOKENS_COLUMN_NAME;
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
    private final InetSocketAddress localNativeTransportAddress;
    private volatile Host host;

    /**
     * Constructs a new {@link CassandraAdapterDelegate} for the given {@code cassandraInstance}
     *
     * @param vertx               the vertx instance
     * @param cassandraInstanceId the cassandra instance identifier
     * @param versionProvider     a Cassandra version provider
     * @param session             the session to the Cassandra database
     * @param jmxClient           the JMX client used to communicate with the Cassandra instance
     * @param sidecarVersion      the version of the Sidecar from the current binary
     * @param host                the Cassandra instance's hostname or ip address as a string
     * @param port                the Cassandra instance's port number
     */
    public CassandraAdapterDelegate(Vertx vertx,
                                    int cassandraInstanceId,
                                    CassandraVersionProvider versionProvider,
                                    CQLSessionProvider session,
                                    JmxClient jmxClient,
                                    String sidecarVersion,
                                    String host,
                                    int port)
    {
        this.vertx = Objects.requireNonNull(vertx);
        this.cassandraInstanceId = cassandraInstanceId;
        this.localNativeTransportAddress = new InetSocketAddress(host, port);
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
        Session activeSession = cqlSessionProvider.get();
        if (activeSession == null)
        {
            LOGGER.info("No local CQL session is available for cassandraInstanceId={}. " +
                        "Cassandra instance is down presumably.", cassandraInstanceId);
            markAsDownAndMaybeNotify();
            return;
        }

        maybeRegisterHostListener(activeSession);

        try
        {
            // NOTE: We cannot use `executeLocal` here as there may be no adapter yet.
            SimpleStatement healthCheckStatement =
            new SimpleStatement("SELECT "
                                + RELEASE_VERSION_COLUMN_NAME + ", "
                                + PARTITIONER_COLUMN_NAME + ", "
                                + DATA_CENTER_COLUMN_NAME + ", "
                                + RPC_ADDRESS_COLUMN_NAME + ", "
                                + RPC_PORT_COLUMN_NAME + ", "
                                + TOKENS_COLUMN_NAME
                                + " FROM system.local");
            Metadata metadata = activeSession.getCluster().getMetadata();
            host = getHost(metadata);
            if (host == null)
            {
                LOGGER.warn("Could not find host in cluster metadata by address and port {}",
                            localNativeTransportAddress);
                return;
            }
            healthCheckStatement.setHost(host);
            healthCheckStatement.setConsistencyLevel(ConsistencyLevel.ONE);
            Row oneResult = activeSession.execute(healthCheckStatement).one();

            // Note that within the scope of this method, we should keep on using the local releaseVersion
            String releaseVersion = oneResult.getString(RELEASE_VERSION_COLUMN_NAME);
            NodeSettings newNodeSettings = NodeSettings.builder()
                                                       .releaseVersion(releaseVersion)
                                                       .partitioner(oneResult.getString(PARTITIONER_COLUMN_NAME))
                                                       .sidecarVersion(sidecarVersion)
                                                       .datacenter(oneResult.getString(DATA_CENTER_COLUMN_NAME))
                                                       .tokens(oneResult.getSet(TOKENS_COLUMN_NAME, String.class))
                                                       .rpcAddress(oneResult.getInet(RPC_ADDRESS_COLUMN_NAME))
                                                       .rpcPort(oneResult.getInt(RPC_PORT_COLUMN_NAME))
                                                       .build();

            if (!newNodeSettings.equals(nodeSettings))
            {
                // Update the nodeSettings cache
                SimpleCassandraVersion previousVersion = currentVersion;
                currentVersion = SimpleCassandraVersion.create(releaseVersion);
                adapter = versionProvider.cassandra(releaseVersion)
                                         .create(cqlSessionProvider, jmxClient, localNativeTransportAddress);
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
            // Unregister the host listener.
            markAsDownAndMaybeNotify();
            maybeUnregisterHostListener(activeSession);
        }
    }

    private Host getHost(Metadata metadata)
    {
        if (host == null)
        {
            synchronized (this)
            {
                if (host == null)
                {
                    host = DriverUtils.getHost(metadata, localNativeTransportAddress);
                }
            }
        }
        return host;
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

    public ResultSet executeLocal(Statement statement)
    {
        return fromAdapter(adapter -> adapter.executeLocal(statement));
    }

    public InetSocketAddress localNativeTransportPort()
    {
        return fromAdapter(ICassandraAdapter::localNativeTransportPort);
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
        runIfThisHost(host, this::healthCheck);
    }

    @Override
    public void onUp(Host host)
    {
        runIfThisHost(host, this::healthCheck);
    }

    @Override
    public void onDown(Host host)
    {
        runIfThisHost(host, this::markAsDownAndMaybeNotify);
    }

    @Override
    public void onRemove(Host host)
    {
        runIfThisHost(host, this::healthCheck);
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
        Session activeSession = cqlSessionProvider.getIfConnected();
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

    private void runIfThisHost(Host host, Runnable runnable)
    {
        if (this.localNativeTransportAddress.equals(host.getEndPoint().resolve()))
        {
            runnable.run();
        }
    }
}
