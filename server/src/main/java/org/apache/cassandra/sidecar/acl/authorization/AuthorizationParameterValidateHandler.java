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

package org.apache.cassandra.sidecar.acl.authorization;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.routes.AbstractHandler;
import org.apache.cassandra.sidecar.routes.RoutingContextUtils;
import org.apache.cassandra.sidecar.snapshots.SnapshotPathBuilder;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

/**
 * Simple handler that extracts authorization parameters keyspace/table parameters from the path, validates them,
 * and then adds them to the context.
 */
@Singleton
public class AuthorizationParameterValidateHandler extends AbstractHandler<QualifiedTableName>
{
    private final SnapshotPathBuilder snapshotPathBuilder;

    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the interface to retrieve instance metadata
     * @param executorPools   the executor pools for blocking executions
     * @param validator       a validator instance to validate Cassandra-specific input
     */
    @Inject
    protected AuthorizationParameterValidateHandler(InstanceMetadataFetcher metadataFetcher,
                                                    ExecutorPools executorPools,
                                                    CassandraInputValidator validator,
                                                    SnapshotPathBuilder snapshotPathBuilder)
    {
        super(metadataFetcher, executorPools, validator);
        this.snapshotPathBuilder = snapshotPathBuilder;
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  @NotNull String host,
                                  SocketAddress remoteAddress,
                                  QualifiedTableName qualifiedTableName)
    {
        RoutingContextUtils.put(context, RoutingContextUtils.SC_QUALIFIED_TABLE_NAME, qualifiedTableName);
    }

    @Override
    protected QualifiedTableName extractParamsOrThrow(RoutingContext context)
    {
        Name tableName = null;
        String tableNameParam = context.pathParam(TABLE_PATH_PARAM);
        if (tableNameParam != null)
        {
            // Remove the tableId for routes that have the tableId as part of the path parameter
            tableName = validator.validateTableName(snapshotPathBuilder.maybeRemoveTableId(tableNameParam));
        }
        return new QualifiedTableName(keyspace(context, false), tableName);
    }
}
