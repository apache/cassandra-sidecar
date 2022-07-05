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

package org.apache.cassandra.sidecar.routes;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;

import static org.apache.cassandra.sidecar.utils.RequestUtils.extractHostAddressWithoutPort;

/**
 * An abstract {@link Handler<RoutingContext>} that provides common functionality for handler
 * implementations.
 */
public abstract class AbstractHandler implements Handler<RoutingContext>
{
    protected static final String INSTANCE_ID = "instanceId";

    protected final InstancesConfig instancesConfig;

    /**
     * Constructs a handler with the provided {@code instancesConfig}
     *
     * @param instancesConfig the instances configuration
     */
    protected AbstractHandler(InstancesConfig instancesConfig)
    {
        this.instancesConfig = instancesConfig;
    }

    /**
     * Returns the host from the path if the requests contains the {@code /instance/} path parameter,
     * otherwise it returns the host parsed from the request.
     *
     * @param context the routing context
     * @return the host for the routing context
     * @throws HttpException when the {@code /instance/} path parameter is {@code null}
     */
    public String getHost(RoutingContext context)
    {
        if (context.request().params().contains(INSTANCE_ID))
        {
            String instanceIdParam = context.request().getParam(INSTANCE_ID);
            if (instanceIdParam == null)
            {
                throw new HttpException(HttpResponseStatus.BAD_REQUEST.code(),
                                        "InstanceId query parameter must be provided");
            }

            try
            {
                int instanceId = Integer.parseInt(instanceIdParam);
                return instancesConfig.instanceFromId(instanceId).host();
            }
            catch (NumberFormatException ex)
            {
                throw new HttpException(HttpResponseStatus.BAD_REQUEST.code(),
                                        "InstanceId query parameter must be a valid integer");
            }
        }
        else
        {
            return extractHostAddressWithoutPort(context.request().host());
        }
    }
}
