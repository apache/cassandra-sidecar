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
import org.apache.cassandra.sidecar.config.LiveMigrationConfiguration;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.LiveMigrationMapException;

import static org.apache.cassandra.sidecar.handlers.AbstractHandler.extractHostAddressWithoutPort;


/**
 * Handler for enabling or disabling live-migration APIs. It helps to enable Live Migration APIs for
 * either source/destination specified in {@link LiveMigrationConfiguration#migrationMap()}.
 */
public class LiveMigrationApiEnableDisableHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationApiEnableDisableHandler.class);

    final LiveMigrationMap liveMigrationMap;
    final InstancesMetadata instancesMetadata;

    @Inject
    public LiveMigrationApiEnableDisableHandler(LiveMigrationMap liveMigrationMap,
                                                InstancesMetadata instancesMetadata)
    {
        this.liveMigrationMap = liveMigrationMap;
        this.instancesMetadata = instancesMetadata;
    }

    /**
     * Enables route if local instance is getting live migrated (source).
     *
     * @param rc - routing context
     */
    public void isSource(RoutingContext rc)
    {
        InstanceMetadata instanceMeta = getLocalInstanceMeta(rc);
        try
        {
            if (liveMigrationMap.isSource(instanceMeta))
            {
                rc.next();
            }
            else
            {
                rc.fail(HttpResponseStatus.NOT_FOUND.code());
            }
        }
        catch (LiveMigrationMapException e)
        {
            LOGGER.error("Failed to check if instance {} is a source in live migration map", instanceMeta.host(), e);
            rc.fail(HttpResponseStatus.SERVICE_UNAVAILABLE.code());
        }
    }

    /**
     * Enables route if local instance is the destination.
     *
     * @param rc - routing context
     */
    public void isDestination(RoutingContext rc)
    {
        InstanceMetadata instanceMeta = getLocalInstanceMeta(rc);
        try
        {
            if (liveMigrationMap.isDestination(instanceMeta))
            {
                rc.next();
            }
            else
            {
                rc.fail(HttpResponseStatus.NOT_FOUND.code());
            }
        }
        catch (LiveMigrationMapException e)
        {
            LOGGER.error("Failed to check if instance {} is a destination in live migration map",
                         instanceMeta.host(), e);
            rc.fail(HttpResponseStatus.SERVICE_UNAVAILABLE.code());
        }
    }

    /**
     * Enables route if local instance is either source or destination in live migration map.
     *
     * @param rc - routing context
     */
    public void isSourceOrDestination(RoutingContext rc)
    {
        InstanceMetadata instanceMeta = getLocalInstanceMeta(rc);
        try
        {
            if (liveMigrationMap.isAny(instanceMeta))
            {
                rc.next();
            }
            else
            {
                rc.fail(HttpResponseStatus.NOT_FOUND.code());
            }
        }
        catch (LiveMigrationMapException e)
        {
            LOGGER.error("Failed to check if instance {} is source or destination in live migration map",
                         instanceMeta.host(), e);
            rc.fail(HttpResponseStatus.SERVICE_UNAVAILABLE.code());
        }
    }

    public void neitherSourceNorDestination(RoutingContext rc)
    {
        InstanceMetadata instanceMeta = getLocalInstanceMeta(rc);
        try
        {
            if (!liveMigrationMap.isAny(instanceMeta))
            {
                rc.next();
            }
            else
            {
                // If the current instance is either source or destination
                rc.fail(HttpResponseStatus.FORBIDDEN.code());
            }
        }
        catch (LiveMigrationMapException e)
        {
            LOGGER.error("Failed to check if instance {} is source or destination in live migration map",
                         instanceMeta.host(), e);
            rc.fail(HttpResponseStatus.SERVICE_UNAVAILABLE.code());
        }
    }

    private InstanceMetadata getLocalInstanceMeta(RoutingContext rc)
    {
        String host = extractHostAddressWithoutPort(rc.request());
        return instancesMetadata.instanceFromHost(host);
    }
}
