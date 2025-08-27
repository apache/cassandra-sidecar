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

package org.apache.cassandra.sidecar.handlers.validations;

import com.datastax.driver.core.KeyspaceMetadata;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.handlers.AbstractHandler;
import org.apache.cassandra.sidecar.routes.RoutingContextUtils;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Validate the request keyspace should exist in Cassandra, when the endpoint
 * contains keyspace name.
 * On successful validation, it stores the fetched {@link KeyspaceMetadata}
 * in the {@link RoutingContext}
 */
@Singleton
public class ValidateKeyspaceExistenceHandler extends AbstractHandler<Name>
{
    @Inject
    public ValidateKeyspaceExistenceHandler(InstanceMetadataFetcher metadataFetcher,
                                          ExecutorPools executorPools,
                                          CassandraInputValidator validator)
    {
        super(metadataFetcher, executorPools, validator);
    }

    @Override
    protected Name extractParamsOrThrow(RoutingContext context)
    {
        return keyspace(context, false);
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  @NotNull String host,
                                  SocketAddress remoteAddress,
                                  Name keyspace)
    {
        if (keyspace == null)
        {
            context.fail(wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Keyspace parameter is required but not provided"));
            return;
        }

        ValidationUtils.validateKeyspaceExists(metadataFetcher, executorPools, host, keyspace.name())
        .onComplete(ar -> {
            if (ar.succeeded())
            {
                // Store metadata in context
                KeyspaceMetadata metadata = ar.result();
                RoutingContextUtils.put(context, RoutingContextUtils.SC_KEYSPACE_METADATA, metadata);
                context.next();
            }
            else
            {
                // Handle failure
                if (ar.cause().getMessage().contains("not found"))
                {
                    context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND, ar.cause().getMessage()));
                }
                else
                {
                    context.fail(ar.cause());
                }
            }
        });
    }
}
