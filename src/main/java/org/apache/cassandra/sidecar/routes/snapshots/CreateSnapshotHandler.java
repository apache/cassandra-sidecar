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

package org.apache.cassandra.sidecar.routes.snapshots;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.StorageOperations;
import org.apache.cassandra.sidecar.common.exceptions.NodeBootstrappingException;
import org.apache.cassandra.sidecar.common.exceptions.SnapshotAlreadyExistsException;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.data.SnapshotRequest;
import org.apache.cassandra.sidecar.routes.AbstractHandler;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.cassandraServiceUnavailable;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * The <b>PUT</b> verb creates a new snapshot for the given keyspace and table
 */
@Singleton
public class CreateSnapshotHandler extends AbstractHandler<SnapshotRequest>
{
    private static final String TTL_QUERY_PARAM = "ttl";

    @Inject
    public CreateSnapshotHandler(InstanceMetadataFetcher metadataFetcher,
                                 CassandraInputValidator validator,
                                 ExecutorPools executorPools)
    {
        super(metadataFetcher, executorPools, validator);
    }

    /**
     * Creates a new snapshot for the given keyspace and table.
     *
     * @param context       the event to handle
     * @param httpRequest   the {@link HttpServerRequest} object
     * @param host          the name of the host
     * @param remoteAddress the remote address that originated the request
     * @param requestParams parameters obtained from the request
     */
    @Override
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               String host,
                               SocketAddress remoteAddress,
                               SnapshotRequest requestParams)
    {
        TaskExecutorPool pool = executorPools.service();
        pool.executeBlocking(promise -> {
                CassandraAdapterDelegate delegate = metadataFetcher.delegate(host);
                if (delegate == null)
                    throw cassandraServiceUnavailable();
                StorageOperations storageOperations = delegate.storageOperations();
                if (storageOperations == null)
                    throw cassandraServiceUnavailable();
                logger.debug("Creating snapshot request={}, remoteAddress={}, instance={}",
                             requestParams, remoteAddress, host);
                Map<String, String> options = requestParams.ttl() != null
                                              ? ImmutableMap.of("ttl", requestParams.ttl())
                                              : ImmutableMap.of();

                storageOperations.takeSnapshot(requestParams.snapshotName(), requestParams.keyspace(),
                                               requestParams.tableName(), options);
                JsonObject jsonObject = new JsonObject()
                                        .put("result", "Success");
                context.json(jsonObject);
            })
            .onFailure(cause -> processFailure(cause, context, host, remoteAddress, requestParams));
    }

    @Override
    protected void processFailure(Throwable cause,
                                  RoutingContext context,
                                  String host,
                                  SocketAddress remoteAddress,
                                  SnapshotRequest requestParams)
    {
        logger.error("SnapshotsHandler failed for request={}, remoteAddress={}, instance={}, method={}",
                     requestParams, remoteAddress, host, context.request().method(), cause);

        if (cause instanceof SnapshotAlreadyExistsException)
        {
            context.fail(wrapHttpException(HttpResponseStatus.CONFLICT, cause.getMessage()));
            return;
        }
        else if (cause instanceof NodeBootstrappingException)
        {
            // Cassandra does not allow taking snapshots while the node is JOINING the ring
            context.fail(wrapHttpException(HttpResponseStatus.SERVICE_UNAVAILABLE,
                                           "The Cassandra instance " + host + " is not available"));
        }
        else if (cause instanceof IllegalArgumentException)
        {
            if (StringUtils.contains(cause.getMessage(),
                                     "Keyspace " + requestParams.keyspace() + " does not exist") ||
                StringUtils.contains(cause.getMessage(),
                                     "Unknown keyspace/cf pair"))
            {
                context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND, cause.getMessage()));
            }
            else
            {
                context.fail(wrapHttpException(HttpResponseStatus.BAD_REQUEST, cause.getMessage()));
            }
            return;
        }
        context.fail(wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Invalid request for " + requestParams));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected SnapshotRequest extractParamsOrThrow(RoutingContext context)
    {
        String ttl = context.request().getParam(TTL_QUERY_PARAM);

        SnapshotRequest snapshotRequest = SnapshotRequest.builder()
                                                         .qualifiedTableName(qualifiedTableName(context))
                                                         .snapshotName(context.pathParam("snapshot"))
                                                         .ttl(ttl)
                                                         .build();
        validate(snapshotRequest);
        return snapshotRequest;
    }

    private void validate(SnapshotRequest request)
    {
        validator.validateSnapshotName(request.snapshotName());
    }
}
