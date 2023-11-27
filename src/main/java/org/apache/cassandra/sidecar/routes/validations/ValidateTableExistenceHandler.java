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

package org.apache.cassandra.sidecar.routes.validations;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.routes.AbstractHandler;
import org.apache.cassandra.sidecar.routes.RoutingContextUtils;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.cassandraServiceUnavailable;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Validate the request table (and the keyspace) should exist in Cassandra, when the endpoint
 * contains keyspace and/or table names.
 * On successful validation, it stores the fetched {@link KeyspaceMetadata} and {@link TableMetadata}
 * in the {@link RoutingContext}
 */
@Singleton
public class ValidateTableExistenceHandler extends AbstractHandler<QualifiedTableName>
{
    @Inject
    public ValidateTableExistenceHandler(InstanceMetadataFetcher metadataFetcher,
                                         ExecutorPools executorPools,
                                         CassandraInputValidator validator)
    {
        super(metadataFetcher, executorPools, validator);
    }

    // It is a validator, and it does not assume values (keyspace and table) are present.
    @Override
    protected QualifiedTableName extractParamsOrThrow(RoutingContext context)
    {
        // note that both keyspace and table are not required
        return qualifiedTableName(context, /* required */ false);
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  String host,
                                  SocketAddress remoteAddress,
                                  QualifiedTableName input)
    {
        if (input.keyspace() == null)
        {
            // no need to validate
            context.next();
            return;
        }

        getKeyspaceMetadata(host, input.keyspace())
        .onFailure(context::fail) // fail the request with the internal server error thrown from getKeyspaceMetadata
        .onSuccess(keyspaceMetadata -> {
            if (keyspaceMetadata == null)
            {
                context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND,
                                               "Keyspace " + input.keyspace() + " was not found"));
                return;
            }

            RoutingContextUtils.put(context, RoutingContextUtils.SC_KEYSPACE_METADATA, keyspaceMetadata);

            String table = input.tableName();
            if (table == null)
            {
                context.next();
                return;
            }

            TableMetadata tableMetadata = keyspaceMetadata.getTable(table);
            if (tableMetadata == null)
            {
                String errMsg = "Table " + input.tableName() + " was not found for keyspace " + input.keyspace();
                context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND, errMsg));
            }
            else
            {
                RoutingContextUtils.put(context, RoutingContextUtils.SC_TABLE_METADATA, tableMetadata);
                // keyspace / [table] exists
                context.next();
            }
        });
    }

    private Future<KeyspaceMetadata> getKeyspaceMetadata(String host, String keyspace)
    {
        return executorPools.service().executeBlocking(promise -> {
            CassandraAdapterDelegate delegate = metadataFetcher.delegate(host);
            Metadata metadata = delegate.metadata();
            if (metadata == null)
            {
                promise.fail(cassandraServiceUnavailable());
            }
            else
            {
                promise.complete(metadata.getKeyspace(keyspace));
            }
        });
    }
}
