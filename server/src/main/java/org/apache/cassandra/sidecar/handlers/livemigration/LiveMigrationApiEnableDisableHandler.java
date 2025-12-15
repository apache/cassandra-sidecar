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

package org.apache.cassandra.sidecar.handlers.livemigration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationStatusTracker;

import static org.apache.cassandra.sidecar.handlers.AbstractHandler.extractHostAddressWithoutPort;


/**
 * Handler for enabling or disabling live migration APIs based on instance role and migration status.
 * <p>
 * <h2>Handler Methods:</h2>
 * <ul>
 *   <li>{@link #isSource(RoutingContext)} - Allows access only if instance is a migration source</li>
 *   <li>{@link #isDestination(RoutingContext)} - Allows access only if instance is a migration destination</li>
 *   <li>{@link #isSourceOrDestination(RoutingContext)} - Allows access if instance is either source or destination</li>
 *   <li>{@link #neitherSourceNorDestination(RoutingContext)} - Allows access only if instance is not involved in migration</li>
 *   <li>{@link #allowIfMigrationNotComplete(RoutingContext)} - Allows access only if migration is not yet completed</li>
 * </ul>
 */
public class LiveMigrationApiEnableDisableHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationApiEnableDisableHandler.class);

    private final LiveMigrationMap liveMigrationMap;
    private final InstancesMetadata instancesMetadata;
    private final LiveMigrationStatusTracker statusTracker;

    @Inject
    public LiveMigrationApiEnableDisableHandler(LiveMigrationMap liveMigrationMap,
                                                InstancesMetadata instancesMetadata,
                                                LiveMigrationStatusTracker statusTracker)
    {
        this.liveMigrationMap = liveMigrationMap;
        this.instancesMetadata = instancesMetadata;
        this.statusTracker = statusTracker;
    }

    /**
     * Enables route if local instance is getting live migrated (source).
     *
     * @param rc routing context
     */
    public void isSource(RoutingContext rc)
    {
        InstanceMetadata instanceMeta = getLocalInstanceMeta(rc);

        liveMigrationMap.isSource(instanceMeta)
                        .onSuccess(isSource -> {
                            if (isSource)
                            {
                                rc.next();
                            }
                            else
                            {
                                rc.fail(HttpResponseStatus.NOT_FOUND.code());
                            }
                        })
                        .onFailure(t -> {
                            LOGGER.error("Failed to check if instance {} is a source in live migration map", instanceMeta.host(), t);
                            rc.fail(HttpResponseStatus.SERVICE_UNAVAILABLE.code());
                        });
    }

    /**
     * Enables route if local instance is the destination.
     *
     * @param rc routing context
     */
    public void isDestination(RoutingContext rc)
    {
        InstanceMetadata instanceMeta = getLocalInstanceMeta(rc);

        liveMigrationMap.isDestination(instanceMeta)
                        .onSuccess(isDestination -> {
                            if (isDestination)
                            {
                                rc.next();
                            }
                            else
                            {
                                rc.fail(HttpResponseStatus.NOT_FOUND.code());
                            }
                        })
                        .onFailure(t -> {
                            LOGGER.error("Failed to check if instance {} is a destination in live migration map",
                                         instanceMeta.host(), t);
                            rc.fail(HttpResponseStatus.SERVICE_UNAVAILABLE.code());
                        });
    }

    /**
     * Enables route if local instance is either source or destination in live migration map.
     *
     * @param rc routing context
     */
    public void isSourceOrDestination(RoutingContext rc)
    {
        InstanceMetadata instanceMeta = getLocalInstanceMeta(rc);
        liveMigrationMap.isAny(instanceMeta)
                        .onSuccess(isAny -> {
                            if (isAny)
                            {
                                rc.next();
                            }
                            else
                            {
                                rc.fail(HttpResponseStatus.NOT_FOUND.code());
                            }
                        })
                        .onFailure(t -> {
                            LOGGER.error("Failed to check if instance {} is source or destination in live migration map",
                                         instanceMeta.host(), t);
                            rc.fail(HttpResponseStatus.SERVICE_UNAVAILABLE.code());
                        });
    }

    /**
     * Enables route if local instance is neither source nor destination in the live migration map.
     *
     * @param rc routing context
     */
    public void neitherSourceNorDestination(RoutingContext rc)
    {
        InstanceMetadata instanceMeta = getLocalInstanceMeta(rc);
        liveMigrationMap.isAny(instanceMeta)
                        .onSuccess(isAny -> {
                            if (!isAny)
                            {
                                rc.next();
                            }
                            else
                            {
                                // If the current instance is either source or destination
                                rc.fail(HttpResponseStatus.FORBIDDEN.code());
                            }
                        })
                        .onFailure(t -> {
                            LOGGER.error("Failed to check if instance {} is source or destination in live migration map",
                                         instanceMeta.host(), t);
                            rc.fail(HttpResponseStatus.SERVICE_UNAVAILABLE.code());
                        });
    }


    /**
     * Enables route if live migration has not been marked as completed for the instance.
     * Routes are allowed only if migration is not yet completed; otherwise returns BAD_REQUEST.
     *
     * @param rc routing context
     */
    public void allowIfMigrationNotComplete(RoutingContext rc)
    {
        InstanceMetadata instanceMeta = getLocalInstanceMeta(rc);

        statusTracker.hasMigrationCompleted(instanceMeta)
                     .onSuccess(completed -> {
                         if (!completed)
                         {
                             rc.next();
                         }
                         else
                         {
                             LOGGER.debug("Live Migration already completed. Cannot proceed with the request.");
                             rc.fail(HttpResponseStatus.BAD_REQUEST.code());
                         }
                     })
                     .onFailure(e -> {
                         LOGGER.error("Failed to check the status of LiveMigration status", e);
                         rc.fail(HttpResponseStatus.SERVICE_UNAVAILABLE.code());
                     });
    }

    private InstanceMetadata getLocalInstanceMeta(RoutingContext rc)
    {
        String host = extractHostAddressWithoutPort(rc.request());
        return instancesMetadata.instanceFromHost(host);
    }
}
