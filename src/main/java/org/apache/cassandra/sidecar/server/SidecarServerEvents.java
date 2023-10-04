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

package org.apache.cassandra.sidecar.server;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;

/**
 * Defines the {@link io.vertx.core.eventbus.EventBus} addresses where different notifications will be published
 * during Sidecar startup/shutdown, as well as CQL connection availability.
 *
 * <p>The messages can be published multiple times depending on whether Sidecar is started or stopped
 * during the lifetime of the application. Implementing consumers will need to deal with this expectation
 * internally.
 * <p>
 * The expectation is that:
 * <ul>
 * <li>{@link #ON_SERVER_START} will happen first
 * <li>{@link #ON_SERVER_STOP} can happen before {@link #ON_ALL_CASSANDRA_CQL_READY}
 * <li>{@link #ON_SERVER_START} can only happen for any subsequent calls only after a {@link #ON_SERVER_STOP} message
 * <li>{@link #ON_ALL_CASSANDRA_CQL_READY} might never happen
 * <li>{@link #ON_CASSANDRA_CQL_READY} can be called multiple times with different cassandraInstanceId values
 * <li>{@link #ON_CASSANDRA_CQL_DISCONNECTED} can be called multiple times with different cassandraInstanceId values
 * </ul>
 * <p>
 * However, implementers should choose to implement methods assuming no guarantees to the event sequence.
 */
@Singleton
public class SidecarServerEvents
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SidecarServerEvents.class);

    /**
     * The {@link io.vertx.core.eventbus.EventBus} address where server start events will be published. Server start
     * will be published whenever Sidecar has successfully started and is ready listening for requests.
     */
    public static final String ON_SERVER_START = SidecarServerEvents.class.getName() + ".ON_SERVER_START";

    /**
     * The {@link io.vertx.core.eventbus.EventBus} address where server stop/shutdown events will be published.
     * Server stop events will be published whenever Sidecar is stopping or shutting down.
     */
    public static final String ON_SERVER_STOP = SidecarServerEvents.class.getName() + ".ON_SERVER_STOP";

    /**
     * The {@link io.vertx.core.eventbus.EventBus} address where events will be published when a CQL connection for
     * a given instance has been established. The instance identifier will be passed as part of the message.
     */
    public static final String ON_CASSANDRA_CQL_READY = SidecarServerEvents.class.getName() + ".ON_CASSANDRA_CQL_READY";

    /**
     * The {@link io.vertx.core.eventbus.EventBus} address where events will be published when a CQL connection for
     * a given instance has been disconnected. The instance identifier will be passed as part of the message.
     */
    public static final String ON_CASSANDRA_CQL_DISCONNECTED = SidecarServerEvents.class.getName()
                                                               + ".ON_CASSANDRA_CQL_DISCONNECTED";

    /**
     * The {@link io.vertx.core.eventbus.EventBus} address where events will be published when all CQL connections
     * for the Sidecar-managed Cassandra instances are available.
     */
    public static final String ON_ALL_CASSANDRA_CQL_READY = SidecarServerEvents.class.getName()
                                                            + ".ON_ALL_CASSANDRA_CQL_READY";

    private final EventBus eventBus;
    private final Set<Integer> cqlReadyInstanceIds = Collections.synchronizedSet(new HashSet<>());

    @Inject
    public SidecarServerEvents(Vertx vertx, InstancesConfig instancesConfig)
    {
        eventBus = vertx.eventBus();
        MessageConsumer<JsonObject> cqlReadyConsumer = eventBus.localConsumer(ON_CASSANDRA_CQL_READY);
        cqlReadyConsumer.handler(message -> {
            cqlReadyInstanceIds.add(message.body().getInteger("cassandraInstanceId"));

            boolean isCqlReadyOnAllInstances = instancesConfig.instances().stream()
                                                              .map(InstanceMetadata::id)
                                                              .allMatch(cqlReadyInstanceIds::contains);
            if (isCqlReadyOnAllInstances)
            {
                cqlReadyConsumer.unregister(); // stop listening to CQL ready events
                notifyAllCassandraCqlAreReady();
                LOGGER.info("CQL is ready for all Cassandra instances. {}", cqlReadyInstanceIds);
            }
        });
    }

    /**
     * Constructs the notification message containing all the Cassandra instance IDs and publishes the message
     * notifying consumers that all the CQL connections are available.
     */
    private void notifyAllCassandraCqlAreReady()
    {
        JsonArray cassandraInstanceIds = new JsonArray();
        cqlReadyInstanceIds.forEach(cassandraInstanceIds::add);
        JsonObject allReadyMessage = new JsonObject()
                                     .put("cassandraInstanceIds", cassandraInstanceIds);

        eventBus.publish(ON_ALL_CASSANDRA_CQL_READY, allReadyMessage);
    }
}
