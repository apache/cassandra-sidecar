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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.exceptions.InsufficientStorageException;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetrics;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.apache.cassandra.sidecar.utils.AsyncFileSystemUtils.ensureSufficientStorage;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * A protection machinery that rejects incoming requests if the used disk space has exceeded the configured threshold.
 * The protection should only be applied to write requests, e.g. UploadSSTable, CreateRestoreSlice, etc.
 */
@Singleton
public class DiskSpaceProtectionHandler extends AbstractHandler<Void>
{
    private final ServiceConfiguration config;

    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher the interface to retrieve instance metadata
     * @param executorPools   the executor pools for blocking executions
     * @param validator       a validator instance to validate Cassandra-specific input
     */
    @Inject
    protected DiskSpaceProtectionHandler(ServiceConfiguration config,
                                         InstanceMetadataFetcher metadataFetcher,
                                         ExecutorPools executorPools,
                                         CassandraInputValidator validator)
    {
        super(metadataFetcher, executorPools, validator);
        this.config = config;
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  String host,
                                  SocketAddress remoteAddress,
                                  Void request)
    {
        InstanceMetadata instance = metadataFetcher.instance(host);
        String stagingDir = instance.stagingDir();
        if (stagingDir == null)
        {
            logger.warn("Cannot resolve the staging dir. instance={}", instance.host());
            context.next();
            return;
        }

        float minimumPercentageRequired = config.ssTableUploadConfiguration().minimumSpacePercentageRequired();
        if (minimumPercentageRequired == 0)
        {
            logger.info("Minimum disk space percentage protection is disabled. " +
                        "It is highly recommend to configure the disk space protection.");
            // since it is disabled, the request is let go.
            context.next();
            return;
        }

        double scaledRequiredUsablePercentage = minimumPercentageRequired / 100.0;

        context
        .vertx()
        .fileSystem()
        .mkdirs(stagingDir)
        .compose(ignored -> ensureSufficientStorage(stagingDir,
                                                    scaledRequiredUsablePercentage,
                                                    executorPools.internal()))
        .onSuccess(ignored -> context.next())
        .onFailure(throwable -> {
            if (throwable instanceof InsufficientStorageException)
            {
                InstanceMetrics metrics = instance.metrics();
                metrics.forResource().insufficientStagingSpaceErrors.metric.mark();
                InsufficientStorageException exception = (InsufficientStorageException) throwable;
                throwable = wrapHttpException(HttpResponseStatus.INSUFFICIENT_STORAGE,
                                              exception.getMessage(),
                                              exception);
            }
            processFailure(throwable, context, host, remoteAddress, request);
        });
    }

    @Override
    protected Void extractParamsOrThrow(RoutingContext context)
    {
        return null;
    }
}
