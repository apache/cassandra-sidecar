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

package org.apache.cassandra.sidecar.modules;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.ProvidesIntoMap;
import org.apache.cassandra.sidecar.handlers.ConnectedClientStatsHandler;
import org.apache.cassandra.sidecar.handlers.GossipInfoHandler;
import org.apache.cassandra.sidecar.handlers.KeyspaceRingHandler;
import org.apache.cassandra.sidecar.handlers.KeyspaceSchemaHandler;
import org.apache.cassandra.sidecar.handlers.ListOperationalJobsHandler;
import org.apache.cassandra.sidecar.handlers.NodeDecommissionHandler;
import org.apache.cassandra.sidecar.handlers.OperationalJobHandler;
import org.apache.cassandra.sidecar.handlers.RingHandler;
import org.apache.cassandra.sidecar.handlers.SchemaHandler;
import org.apache.cassandra.sidecar.handlers.StreamStatsHandler;
import org.apache.cassandra.sidecar.handlers.TableStatsHandler;
import org.apache.cassandra.sidecar.handlers.TokenRangeReplicaMapHandler;
import org.apache.cassandra.sidecar.handlers.cassandra.NodeSettingsHandler;
import org.apache.cassandra.sidecar.handlers.validations.ValidateTableExistenceHandler;
import org.apache.cassandra.sidecar.modules.multibindings.KeyClassMapKey;
import org.apache.cassandra.sidecar.modules.multibindings.VertxRouteMapKeys;
import org.apache.cassandra.sidecar.routes.RouteBuilder;
import org.apache.cassandra.sidecar.routes.VertxRoute;

/**
 * Provides the capability to query and invoke Cassandra operations
 */
public class CassandraOperationsModule extends AbstractModule
{
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraConnectedClientStatsRouteKey.class)
    VertxRoute cassandraConnectedClientStatsRoute(RouteBuilder.Factory factory,
                                                  ConnectedClientStatsHandler connectedClientStatsHandler)
    {
        return factory.buildRouteWithHandler(connectedClientStatsHandler);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraOperationalJobRouteKey.class)
    VertxRoute cassandraOperationalJobRoute(RouteBuilder.Factory factory,
                                            OperationalJobHandler operationalJobHandler)
    {
        return factory.buildRouteWithHandler(operationalJobHandler);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.ListCassandraOperationalJobRouteKey.class)
    VertxRoute listCassandraOperationalJobRoute(RouteBuilder.Factory factory,
                                                ListOperationalJobsHandler listOperationalJobsHandler)
    {
        return factory.buildRouteWithHandler(listOperationalJobsHandler);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraNodeDecommissionRouteKey.class)
    VertxRoute cassandraNodeDecommissionRoute(RouteBuilder.Factory factory,
                                              NodeDecommissionHandler nodeDecommissionHandler)
    {
        return factory.buildRouteWithHandler(nodeDecommissionHandler);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraStreamStatsRouteKey.class)
    VertxRoute cassandraStreamStatsRoute(RouteBuilder.Factory factory,
                                         StreamStatsHandler streamStatsHandler)
    {
        return factory.buildRouteWithHandler(streamStatsHandler);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraNodeSettingsRouteKey.class)
    VertxRoute cassandraNodeSettings(RouteBuilder.Factory factory,
                                     NodeSettingsHandler nodeSettingsHandler)
    {
        // Node settings endpoint is not Access protected. Any user who can log in into Cassandra is able to view
        // node settings information. Since sidecar and cassandra share list of authenticated identities, sidecar's
        // authenticated users can also read node settings information.
        return factory.builderForUnauthorizedRoute()
                      .handler(nodeSettingsHandler)
                      .build();
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.AllKeyspacesSchemaRouteKey.class)
    VertxRoute cassandraSchemaRoute(RouteBuilder.Factory factory,
                                    SchemaHandler schemaHandler)
    {
        return factory.buildRouteWithHandler(schemaHandler);
    }

    @Deprecated
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.DeprecatedAllKeyspacesSchemaRouteKey.class)
    VertxRoute deprecatedCassandraSchemaRoute(RouteBuilder.Factory factory,
                                              SchemaHandler schemaHandler)
    {
        return factory.buildRouteWithHandler(schemaHandler);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.KeyspaceSchemaRouteKey.class)
    VertxRoute cassandraKeyspaceSchemaRoute(RouteBuilder.Factory factory,
                                            KeyspaceSchemaHandler keyspaceSchemaHandler)
    {
        return factory.buildRouteWithHandler(keyspaceSchemaHandler);
    }

    @Deprecated
    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.DeprecatedKeyspaceSchemaRouteKey.class)
    VertxRoute deprecatedCassandraKeyspaceSchemaRoute(RouteBuilder.Factory factory,
                                                      KeyspaceSchemaHandler keyspaceSchemaHandler)
    {
        return factory.buildRouteWithHandler(keyspaceSchemaHandler);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraRingRouteKey.class)
    VertxRoute cassandraRingRoute(RouteBuilder.Factory factory,
                                  RingHandler ringHandler)
    {
        return factory.buildRouteWithHandler(ringHandler);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraRingWithKeyspaceRouteKey.class)
    VertxRoute cassandraRingWithKeyspaceRoute(RouteBuilder.Factory factory,
                                              KeyspaceRingHandler keyspaceRingHandler)
    {
        return factory.buildRouteWithHandler(keyspaceRingHandler);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraTokenRangeReplicaMapRouteKey.class)
    VertxRoute cassandraTokenRangeReplicaMapRoute(RouteBuilder.Factory factory,
                                                  TokenRangeReplicaMapHandler tokenRangeReplicaMapHandler)
    {
        return factory.buildRouteWithHandler(tokenRangeReplicaMapHandler);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.CassandraGossipInfoRouteKey.class)
    VertxRoute cassandraGossipInfoRoute(RouteBuilder.Factory factory,
                                        GossipInfoHandler gossipInfoHandler)
    {
        return factory.buildRouteWithHandler(gossipInfoHandler);
    }

    @ProvidesIntoMap
    @KeyClassMapKey(VertxRouteMapKeys.TableStatsRouteKey.class)
    VertxRoute tableStatsRoute(RouteBuilder.Factory factory,
                               ValidateTableExistenceHandler validateTableExistenceHandler,
                               TableStatsHandler tableStatsHandler)
    {
        return factory.builderForRoute()
                      .handler(validateTableExistenceHandler)
                      .handler(tableStatsHandler).build();
    }
}
