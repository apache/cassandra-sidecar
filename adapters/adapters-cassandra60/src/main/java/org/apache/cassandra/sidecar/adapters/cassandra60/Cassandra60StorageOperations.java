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

package org.apache.cassandra.sidecar.adapters.cassandra60;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.management.InstanceNotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.adapters.base.RingProvider;
import org.apache.cassandra.sidecar.adapters.base.TokenRangeReplicaProvider;
import org.apache.cassandra.sidecar.adapters.cassandra50.Cassandra50StorageOperations;
import org.apache.cassandra.sidecar.adapters.cassandra60.jmx.SnapshotManagerJmxOperations;
import org.apache.cassandra.sidecar.common.server.JmxClient;
import org.apache.cassandra.sidecar.common.server.StorageOperations;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.server.utils.ThrowableUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.Objects.requireNonNull;
import static org.apache.cassandra.sidecar.adapters.cassandra60.jmx.SnapshotManagerJmxOperations.SNAPSHOT_MANAGER_OBJ_NAME;

/**
 * An implementation of the {@link StorageOperations} that interfaces with Cassandra 6.0 and later.
 * <p>
 * Cassandra 6.0 deprecated the snapshot methods on the Storage Service MBean in favour of the Snapshot Manager
 * MBean, so the snapshot operations are routed there.
 * <p>
 * Only {@code CassandraDaemon} registers the Snapshot Manager MBean. A node that starts without it, such as an
 * in-JVM dtest node, does not export it, so this class falls back to the deprecated methods, which 6.0 keeps.
 * Both routes reach the same Snapshot Manager, so they behave alike.
 * <p>
 * The fallback holds for the life of the instance, which the delegate rebuilds on a JMX reconnect. A node that
 * registers the MBean later in its own startup therefore keeps the deprecated route until the next reconnect.
 */
public class Cassandra60StorageOperations extends Cassandra50StorageOperations
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Cassandra60StorageOperations.class);

    private final AtomicBoolean snapshotManagerMBeanAbsent = new AtomicBoolean(false);

    /**
     * Creates a new instance with the provided {@link JmxClient} and {@link DnsResolver}
     *
     * @param jmxClient   the JMX client used to communicate with the Cassandra instance
     * @param dnsResolver the DNS resolver used to lookup replicas
     */
    public Cassandra60StorageOperations(JmxClient jmxClient, DnsResolver dnsResolver)
    {
        super(jmxClient, dnsResolver);
    }

    /**
     * This constructor is exposed for extensibility.
     *
     * @param jmxClient                 the JMX client used to communicate with the Cassandra instance
     * @param ringProvider              the ring provider instance
     * @param tokenRangeReplicaProvider the token range replica provider
     */
    public Cassandra60StorageOperations(JmxClient jmxClient,
                                        RingProvider ringProvider,
                                        TokenRangeReplicaProvider tokenRangeReplicaProvider)
    {
        super(jmxClient, ringProvider, tokenRangeReplicaProvider);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void takeSnapshot(@NotNull String tag,
                             @NotNull String keyspace,
                             @NotNull String table,
                             @Nullable Map<String, String> options)
    {
        requireNonNull(tag, "snapshot tag must be non-null");
        requireNonNull(keyspace, "keyspace must be non-null");
        requireNonNull(table, "table must be non-null");
        // SnapshotOptions.userSnapshot reads the map without a null check, so normalise before either route
        Map<String, String> opts = null != options ? options : Collections.emptyMap();
        viaSnapshotManager(tag, keyspace, table,
                           snapshotManager -> snapshotManager.takeSnapshot(tag, opts, keyspace + "." + table),
                           () -> super.takeSnapshot(tag, keyspace, table, opts));
    }

    /**
     * {@inheritDoc}
     * <p>
     * The Snapshot Manager options map filters by age only, so {@code table} is still ignored.
     */
    @Override
    public void clearSnapshot(@NotNull String tag, @NotNull String keyspace, @NotNull String table)
    {
        requireNonNull(tag, "snapshot tag must be non-null");
        requireNonNull(keyspace, "keyspace must be non-null");
        requireNonNull(table, "table must be non-null");
        LOGGER.debug("Table is not supported by Cassandra JMX endpoints. " +
                     "Clearing snapshot with tag={} and keyspace={}; table={} is ignored", tag, keyspace, table);
        viaSnapshotManager(tag, keyspace, table,
                           snapshotManager -> snapshotManager.clearSnapshot(tag, Collections.emptyMap(), keyspace),
                           () -> super.clearSnapshot(tag, keyspace, table));
    }

    private void viaSnapshotManager(String tag, String keyspace, String table,
                                    ThrowableUtils.Consumer<SnapshotManagerJmxOperations> operation,
                                    Runnable fallback)
    {
        try
        {
            if (!snapshotManagerMBeanAbsent.get())
            {
                try
                {
                    operation.accept(jmxClient.proxy(SnapshotManagerJmxOperations.class, SNAPSHOT_MANAGER_OBJ_NAME));
                    return;
                }
                catch (RuntimeException e)
                {
                    if (!isSnapshotManagerMBeanAbsent(e))
                    {
                        throw e;
                    }
                }
            }
            fallback.run();
        }
        catch (Exception e)
        {
            RuntimeException translated = classifySnapshotFailure(e, tag, keyspace, table);
            if (null != translated)
            {
                throw translated;
            }
            throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
        }
    }

    /**
     * Remembers the answer, so that later calls skip the failing round trip. Logs one warning per instance.
     *
     * @return {@code true} when the node does not export the Snapshot Manager MBean
     */
    private boolean isSnapshotManagerMBeanAbsent(@NotNull RuntimeException e)
    {
        if (null == ThrowableUtils.getCause(e, InstanceNotFoundException.class))
        {
            return false;
        }
        if (snapshotManagerMBeanAbsent.compareAndSet(false, true))
        {
            LOGGER.warn("The node does not export the {} MBean. Using the deprecated snapshot methods of the "
                        + "Storage Service MBean instead", SNAPSHOT_MANAGER_OBJ_NAME, e);
        }
        return true;
    }

    @Override
    protected boolean messageContains(@NotNull Throwable throwable, @NotNull String text)
    {
        // 6.0 can leave the snapshot task failure message only in the cause.
        return null != ThrowableUtils.getCause(throwable, t -> null != t.getMessage() && t.getMessage().contains(text));
    }
}
