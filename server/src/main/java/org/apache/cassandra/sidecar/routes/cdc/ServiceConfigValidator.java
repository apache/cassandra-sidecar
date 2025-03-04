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
package org.apache.cassandra.sidecar.routes.cdc;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.json.JsonObject;
import org.apache.cassandra.sidecar.common.request.Service;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Certain validations on payload and services have to be done before updating or deleting
 * configs for services in "configs" table. {@link ServiceConfigValidator} has static
 * utility methods for some of those validations.
 */
@Singleton
public class ServiceConfigValidator
{
    public Service validateAndGet(String requestService)
    {
        try
        {
            return Service.withName(requestService);
        }
        catch (Exception e)
        {
            Set<String> services = Stream.of(Service.values()).map(v -> v.serviceName).collect(Collectors.toSet());
            String supportedServices = String.join(", ", services);
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Invalid service provided. Supported services: "
                    + supportedServices);
        }
    }

    public void validateConfig(JsonObject payload)
    {
        try
        {
            payload.getJsonObject(ConfigPayloadParams.CONFIG).getMap();
        }
        catch (ClassCastException ex)
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Invalid configuration provided");
        }
    }

    public void validatePayload(JsonObject payload)
    {
        if (!payload.containsKey(ConfigPayloadParams.CONFIG))
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Invalid request payload. "
                    + "config needs to be passed");
        }
    }
}
