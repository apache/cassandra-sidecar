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
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.remote.JMXConnectionNotification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
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
import org.apache.cassandra.sidecar.common.utils.DriverUtils;
import org.apache.cassandra.sidecar.utils.CassandraVersionProvider;
import org.apache.cassandra.sidecar.utils.SimpleCassandraVersion;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.sidecar.adapters.base.EndpointSnitchJmxOperations.ENDPOINT_SNITCH_INFO_OBJ_NAME;
import static org.apache.cassandra.sidecar.adapters.base.StorageJmxOperations.STORAGE_SERVICE_OBJ_NAME;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CASSANDRA_CQL_DISCONNECTED;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CASSANDRA_CQL_READY;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CASSANDRA_JMX_DISCONNECTED;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CASSANDRA_JMX_READY;


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
    private final DriverUtils driverUtils;
    private final String sidecarVersion;
    private final CassandraVersionProvider versionProvider;
    private final CQLSessionProvider cqlSessionProvider;
    private final JmxClient jmxClient;
    private final JmxNotificationListener notificationListener;
    private SimpleCassandraVersion currentVersion;
    private ICassandraAdapter adapter;
    private volatile boolean isNativeUp = false;
    private volatile NodeSettings nodeSettingsFromJmx = null;
    private final AtomicBoolean registered = new AtomicBoolean(false);
    private final AtomicBoolean isHealthCheckActive = new AtomicBoolean(false);
    private final InetSocketAddress localNativeTransportAddress;
    private volatile Host host;
    private volatile boolean closed = false;

    /**
     * Constructs a new {@link CassandraAdapterDelegate} for the given {@code cassandraInstance}
     *
     * @param vertx               the vertx instance
     * @param cassandraInstanceId the cassandra instance identifier
     * @param versionProvider     a Cassandra version provider
     * @param session             the session to the Cassandra database
     * @param jmxClient           the JMX client used to communicate with the Cassandra instance
     * @param driverUtils         a wrapper that exposes Cassandra driver utilities
     * @param sidecarVersion      the version of the Sidecar from the current binary
     * @param host                the Cassandra instance's hostname or ip address as a string
     * @param port                the Cassandra instance's port number
     */
    public CassandraAdapterDelegate(Vertx vertx,
                                    int cassandraInstanceId,
                                    CassandraVersionProvider versionProvider,
                                    CQLSessionProvider session,
                                    JmxClient jmxClient,
                                    DriverUtils driverUtils,
                                    String sidecarVersion,
                                    String host,
                                    int port)
    {
        this.vertx = Objects.requireNonNull(vertx);
        this.cassandraInstanceId = cassandraInstanceId;
        this.driverUtils = driverUtils;
        this.localNativeTransportAddress = new InetSocketAddress(host, port);
        this.sidecarVersion = sidecarVersion;
        this.versionProvider = versionProvider;
        this.cqlSessionProvider = session;
        this.jmxClient = jmxClient;
        notificationListener = initializeJmxListener();
    }

    protected JmxNotificationListener initializeJmxListener()
    {
        JmxNotificationListener notificationListener = new JmxNotificationListener();
        this.jmxClient.registerListener(notificationListener);
        return notificationListener;
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
     * synchronized, so we don't flood the DB with version requests
     *
     * <p>If the healthcheck determines we've changed versions, it should load the proper adapter</p>
     */
    public void healthCheck()
    {
        if (closed)
        {
            LOGGER.debug("Skipping health check for cassandraInstanceId={}. Delegate is closed", cassandraInstanceId);
            return;
        }

        if (isHealthCheckActive.compareAndSet(false, true))
        {
            try
            {
                jmxHealthCheck();
                nativeProtocolHealthCheck();
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

    /**
     * Performs health checks by utilizing the JMX protocol. It uses a small subset of the exposed mBeans to
     * collect information needed to populate the {@link NodeSettings} object.
     */
    protected void jmxHealthCheck()
    {
        try
        {
            NodeSettings newNodeSettings = newNodeSettingsFromJmx();
            if (!newNodeSettings.equals(nodeSettingsFromJmx))
            {
                // Update the nodeSettings cache
                SimpleCassandraVersion previousVersion = currentVersion;
                currentVersion = SimpleCassandraVersion.create(newNodeSettings.releaseVersion());
                adapter = versionProvider.cassandra(newNodeSettings.releaseVersion())
                                         .create(cqlSessionProvider, jmxClient, localNativeTransportAddress);
                nodeSettingsFromJmx = newNodeSettings;
                LOGGER.info("Cassandra version change detected (from={} to={}) for cassandraInstanceId={}. " +
                            "New adapter loaded={}", previousVersion, currentVersion, cassandraInstanceId, adapter);

                notifyJmxConnection();
            }
            LOGGER.debug("Cassandra version {}", newNodeSettings.releaseVersion());
        }
        catch (RuntimeException e)
        {
            LOGGER.error("Unable to connect JMX to Cassandra instance {}", cassandraInstanceId, e);
            // The cassandra node JMX connectivity is unavailable.
            markJmxDownAndMaybeNotifyDisconnection();
        }
    }

    /**
     * Performs health checks by utilizing the native protocol
     */
    protected void nativeProtocolHealthCheck()
    {
        Session activeSession = cqlSessionProvider.get();
        if (activeSession == null)
        {
            LOGGER.info("No local CQL session is available for cassandraInstanceId={}. " +
                        "Cassandra instance is down presumably.", cassandraInstanceId);
            markNativeDownAndMaybeNotifyDisconnection();
            return;
        }

        maybeRegisterHostListener(activeSession);

        try
        {
            // NOTE: We cannot use `executeLocal` here as there may be no adapter yet.
            SimpleStatement healthCheckStatement = new SimpleStatement("SELECT release_version FROM system.local");
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
            Row row = activeSession.execute(healthCheckStatement).one();

            if (row != null)
            {
                if (!isNativeUp)
                {
                    isNativeUp = true;
                    notifyNativeConnection();
                }
            }
            else
            {
                // This should never happen but added for completeness
                LOGGER.error("Expected to query the release_version from system.local but encountered null {}",
                             cassandraInstanceId);
                // The cassandra native protocol connection to the node is down.
                markNativeDownAndMaybeNotifyDisconnection();
                // Unregister the host listener.
                maybeUnregisterHostListener(activeSession);
            }
        }
        catch (IllegalArgumentException | NoHostAvailableException e)
        {
            LOGGER.error("Unexpected error querying Cassandra instance {}", cassandraInstanceId, e);
            // The cassandra native protocol connection to the node is down.
            markNativeDownAndMaybeNotifyDisconnection();
            // Unregister the host listener.
            maybeUnregisterHostListener(activeSession);
        }
    }

    protected NodeSettings newNodeSettingsFromJmx()
    {
        LimitedStorageOperations storageOperations =
        jmxClient.proxy(LimitedStorageOperations.class, STORAGE_SERVICE_OBJ_NAME);
        LimitedEndpointSnitchOperations endpointSnitchOperations =
        jmxClient.proxy(LimitedEndpointSnitchOperations.class, ENDPOINT_SNITCH_INFO_OBJ_NAME);

        String releaseVersion = storageOperations.getReleaseVersion();
        String partitionerName = storageOperations.getPartitionerName();
        List<String> tokens = maybeGetTokens(storageOperations);
        String dataCenter = endpointSnitchOperations.getDatacenter();

        return NodeSettings.builder()
                           .releaseVersion(releaseVersion)
                           .partitioner(partitionerName)
                           .sidecarVersion(sidecarVersion)
                           .datacenter(dataCenter)
                           .tokens(new LinkedHashSet<>(tokens))
                           .rpcAddress(localNativeTransportAddress.getAddress())
                           .rpcPort(localNativeTransportAddress.getPort())
                           .build();
    }

    /**
     * Attempts to return the tokens assigned to the Cassandra instance.
     *
     * @param storageOperations the interface to perform the operations
     * @return the list of tokens assigned to the Cassandra instance
     */
    protected List<String> maybeGetTokens(LimitedStorageOperations storageOperations)
    {
        try
        {
            return storageOperations.getTokens();
        }
        catch (AssertionError aex)
        {
            // On a joining node, the JMX call will fail with an AssertionError; we catch this scenario to prevent
            // failure and just return an empty list of tokens. This is technically correct, because the node, while
            // joining, doesn't actually own any tokens until it has successfully completed joining.
            return Collections.emptyList();
        }
    }

    protected Host getHost(Metadata metadata)
    {
        if (host == null)
        {
            synchronized (this)
            {
                if (host == null)
                {
                    host = driverUtils.getHost(metadata, localNativeTransportAddress);
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
     * Returns the cached node settings value obtained during scheduled health checks. This method does not delegate
     * to the internal adapter, as the information is retrieved on the configured health check interval.
     *
     * @return a cached {@link NodeSettings}. The returned value will be {@code null} when no JMX connection is
     * established
     */
    @Nullable
    @Override
    public NodeSettings nodeSettings()
    {
        return nodeSettingsFromJmx;
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
        runIfThisHost(host, this::markNativeDownAndMaybeNotifyDisconnection);
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

    /**
     * @return {@code true} if the native protocol is enabled on the Cassandra instance, {@code false} otherwise
     */
    public boolean isNativeUp()
    {
        return isNativeUp;
    }

    /**
     * @return {@code true} if JMX connectivity has been established to the Cassandra instance, {@code false} otherwise
     */
    public boolean isJmxUp()
    {
        return nodeSettingsFromJmx != null;
    }

    public void close()
    {
        closed = true;
        markNativeDownAndMaybeNotifyDisconnection();
        markJmxDownAndMaybeNotifyDisconnection();
        Session activeSession = cqlSessionProvider.getIfConnected();
        if (activeSession != null)
        {
            maybeUnregisterHostListener(activeSession);
        }
        if (jmxClient != null)
        {
            jmxClient.unregisterListener(notificationListener);
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

    protected void notifyJmxConnection()
    {
        JsonObject connectMessage = new JsonObject()
                                    .put("cassandraInstanceId", cassandraInstanceId);
        vertx.eventBus().publish(ON_CASSANDRA_JMX_READY.address(), connectMessage);
        LOGGER.info("JMX connected to cassandraInstanceId={}", cassandraInstanceId);
    }

    protected void notifyNativeConnection()
    {
        JsonObject connectMessage = new JsonObject()
                                    .put("cassandraInstanceId", cassandraInstanceId);
        vertx.eventBus().publish(ON_CASSANDRA_CQL_READY.address(), connectMessage);
        LOGGER.info("CQL connected to cassandraInstanceId={}", cassandraInstanceId);
    }

    protected void markNativeDownAndMaybeNotifyDisconnection()
    {
        boolean wasCqlConnected = isNativeUp;
        isNativeUp = false;
        if (wasCqlConnected)
        {
            JsonObject disconnectMessage = new JsonObject()
                                           .put("cassandraInstanceId", cassandraInstanceId);
            vertx.eventBus().publish(ON_CASSANDRA_CQL_DISCONNECTED.address(), disconnectMessage);
            LOGGER.info("CQL disconnection from cassandraInstanceId={}", cassandraInstanceId);
        }
    }

    protected void markJmxDownAndMaybeNotifyDisconnection()
    {
        NodeSettings currentNodeSettings = nodeSettingsFromJmx;
        nodeSettingsFromJmx = null;
        currentVersion = null;
        adapter = null;
        if (currentNodeSettings != null)
        {
            JsonObject disconnectMessage = new JsonObject()
                                           .put("cassandraInstanceId", cassandraInstanceId);
            vertx.eventBus().publish(ON_CASSANDRA_JMX_DISCONNECTED.address(), disconnectMessage);
            LOGGER.info("JMX disconnection from cassandraInstanceId={}", cassandraInstanceId);
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

    /**
     * A {@link NotificationListener} implementation that reacts to {@link JMXConnectionNotification} notifications
     * and updates the state of the JMX connection internally.
     */
    protected class JmxNotificationListener implements NotificationListener
    {
        @Override
        public void handleNotification(Notification notification, Object handback)
        {
            if (notification instanceof JMXConnectionNotification)
            {
                JMXConnectionNotification connectNotice = (JMXConnectionNotification) notification;
                String type = connectNotice.getType();
                switch (type)
                {
                    case JMXConnectionNotification.OPENED:
                        // Do not notify here as we may not have set up our own delegate yet
                        // Instead, run the JMX Health Check, which will notify once we have
                        // created or updated the adapter.
                        jmxHealthCheck();
                        break;

                    case JMXConnectionNotification.CLOSED:
                    case JMXConnectionNotification.FAILED:
                    case JMXConnectionNotification.NOTIFS_LOST:
                        markJmxDownAndMaybeNotifyDisconnection();
                        break;

                    default:
                        LOGGER.warn("Encountered unexpected JMX notification type={}", type);
                        break;
                }
            }
        }
    }

    /**
     * Limited StorageOperations to obtain information required for node settings. Interface visibility is public
     * because JMX proxy works on public interfaces only.
     */
    public interface LimitedStorageOperations
    {
        /**
         * Fetch a string representation of the Cassandra version.
         *
         * @return A string representation of the Cassandra version.
         */
        String getReleaseVersion();

        /**
         * @return the cluster partitioner
         */
        String getPartitionerName();

        /**
         * Fetch string representations of the tokens for this node.
         *
         * @return a collection of tokens formatted as strings
         */
        List<String> getTokens();
    }

    /**
     * Limited standard Snitch info to obtain information required for node settings. Interface visibility is public
     * because JMX proxy works on public interfaces only.
     */
    public interface LimitedEndpointSnitchOperations
    {
        /**
         * @return the Datacenter name depending on the respective snitch used for this node
         */
        String getDatacenter();
    }
}
