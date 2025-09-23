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

package org.apache.cassandra.sidecar.handlers.role;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.commons.lang3.StringUtils;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.adapters.base.exception.OperationUnavailableException;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.request.GenerateRoleRequest;
import org.apache.cassandra.sidecar.common.request.data.GenerateRoleRequestPayload;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.handlers.AbstractHandler;
import org.apache.cassandra.sidecar.handlers.AccessProtected;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static java.util.stream.Collectors.toMap;
import static org.apache.cassandra.sidecar.common.request.data.GenerateRoleRequestPayload.PASSWORD_GENERATION_KEY_NAME;
import static org.apache.cassandra.sidecar.common.request.data.GenerateRoleRequestPayload.ROLE_NAME_OPTIONS_KEY_NAME;

@Singleton
public class RoleGenerationHandler extends AbstractHandler<GenerateRoleRequest> implements AccessProtected
{
    private static final ParameterPredicate PAYLOAD_PARAMETER_ACCEPTANCE_PREDICATE = new ParameterPredicate();

    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the interface to retrieve instance metadata
     * @param executorPools   the executor pools for blocking executions
     * @param validator       a validator instance to validate Cassandra-specific input
     */
    @Inject
    public RoleGenerationHandler(InstanceMetadataFetcher metadataFetcher, ExecutorPools executorPools, CassandraInputValidator validator)
    {
        super(metadataFetcher, executorPools, validator);
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  @NotNull String host,
                                  SocketAddress remoteAddress,
                                  GenerateRoleRequest request)
    {
        executorPools.service()
                     .executeBlocking(() ->
                                      {
                                          CassandraAdapterDelegate delegate = metadataFetcher.delegate(host);

                                          if (delegate.version().major < 5 && !request.requestBody().sidecarGeneration)
                                              throw new OperationUnavailableException("Cassandra node has to be at least 6 to execute server-side generation.");

                                          return delegate.rolesOperations().generateRole(request.requestBody());
                                      })
                     .onSuccess(context::json)
                     .onFailure(cause -> processFailure(cause, context, host, remoteAddress, request));
    }

    @Override
    protected GenerateRoleRequest extractParamsOrThrow(RoutingContext context)
    {
        boolean passwordGeneration = false;
        boolean sidecarGeneration = false;
        boolean login = false;
        boolean superuser = false;
        Map<String, String> config = new HashMap<>();

        if (context.body() != null)
        {
            JsonObject jsonObject = context.body().asJsonObject();
            if (jsonObject != null)
            {
                JsonObject configObject = jsonObject.getJsonObject(ROLE_NAME_OPTIONS_KEY_NAME);
                if (configObject != null)
                {
                    config = configObject.getMap()
                                         .entrySet()
                                         .stream()
                                         .filter(PAYLOAD_PARAMETER_ACCEPTANCE_PREDICATE)
                                         .collect(toMap(Map.Entry::getKey, e -> (String) e.getValue()));
                }

                if (jsonObject.containsKey(PASSWORD_GENERATION_KEY_NAME))
                    passwordGeneration = jsonObject.getBoolean(GenerateRoleRequestPayload.PASSWORD_GENERATION_KEY_NAME);

                if (jsonObject.containsKey(GenerateRoleRequestPayload.SIDECAR_GENERATION_KEY_NAME))
                    sidecarGeneration = jsonObject.getBoolean(GenerateRoleRequestPayload.SIDECAR_GENERATION_KEY_NAME);

                if (jsonObject.containsKey(GenerateRoleRequestPayload.LOGIN_KEY_NAME))
                    login = jsonObject.getBoolean(GenerateRoleRequestPayload.LOGIN_KEY_NAME);

                if (jsonObject.containsKey(GenerateRoleRequestPayload.SUPERUSER_KEY_NAME))
                    superuser = jsonObject.getBoolean(GenerateRoleRequestPayload.SUPERUSER_KEY_NAME);
            }
        }

        return new GenerateRoleRequest(new GenerateRoleRequestPayload(config,
                                                                      sidecarGeneration,
                                                                      passwordGeneration,
                                                                      login,
                                                                      superuser));
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.ROLE_CREATION.toAuthorization());
    }

    private static class ParameterPredicate implements Predicate<Map.Entry<String, Object>>
    {
        public boolean test(Map.Entry<String, Object> entry)
        {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (!(value instanceof String))
                return false;
            String valueString = (String) value;

            // underscores are allowed either in names of values but nothing else
            String keyWithoutUnderscore = key.replaceAll("_", "");

            // key can not be numeric
            if (!StringUtils.isAlpha(keyWithoutUnderscore))
                return false;

            String valueWithoutUnderscore = valueString.replaceAll("_", "");
            return StringUtils.isAlpha(valueWithoutUnderscore) || isInteger(valueString);
        }

        private boolean isInteger(String value)
        {
            try
            {
                Integer.parseInt(value);
                return true;
            }
            catch (Throwable t)
            {
                return false;
            }
        }
    }
}
