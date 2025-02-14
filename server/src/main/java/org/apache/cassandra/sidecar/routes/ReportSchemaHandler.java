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

import java.util.Collections;
import java.util.Set;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.datahub.SchemaReporter;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An implementation of {@link AbstractHandler} used to trigger an immediate,
 * synchronous conversion and report of the current schema
 */
@Singleton
public class ReportSchemaHandler extends AbstractHandler<Void> implements AccessProtected
{
    @NotNull
    private final SchemaReporter schemaReporter;

    /**
     * Constructs a new instance of {@link ReportSchemaHandler} using the provided instances
     * of {@link InstanceMetadataFetcher}, {@link ExecutorPools}, and {@link SchemaReporter}
     *
     * @param metadata the metadata fetcher
     * @param executor executor pools for blocking executions
     * @param reporter executor pools for blocking executions
     */
    @Inject
    public ReportSchemaHandler(@NotNull InstanceMetadataFetcher metadata,
                               @NotNull ExecutorPools executor,
                               @NotNull SchemaReporter reporter)
    {
        super(metadata, executor, null);

        schemaReporter = reporter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @NotNull
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.REPORT_SCHEMA.toAuthorization());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nullable
    protected Void extractParamsOrThrow(@NotNull RoutingContext context)
    {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void handleInternal(@NotNull RoutingContext context,
                                  @NotNull HttpServerRequest http,
                                  @NotNull String host,
                                  @NotNull SocketAddress address,
                                  @Nullable Void request)
    {
        executorPools.service()
                     .runBlocking(() -> metadataFetcher.runOnFirstAvailableInstance(instance ->
                            schemaReporter.process(instance.delegate().metadata())))
                     .onSuccess(context::json)
                     .onFailure(throwable -> processFailure(throwable, context, host, address, request));
    }
}
