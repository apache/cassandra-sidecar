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

package org.apache.cassandra.sidecar.routes.sstableuploads;

import java.nio.file.NoSuchFileException;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.common.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.common.TableOperations;
import org.apache.cassandra.sidecar.common.data.SSTableImportResponse;
import org.apache.cassandra.sidecar.common.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.data.SSTableImportRequest;
import org.apache.cassandra.sidecar.routes.AbstractHandler;
import org.apache.cassandra.sidecar.utils.CacheFactory;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.SSTableImporter;
import org.apache.cassandra.sidecar.utils.SSTableUploadsPathBuilder;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.cassandraServiceUnavailable;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Imports SSTables, that have been previously uploaded, into Cassandra
 */
public class SSTableImportHandler extends AbstractHandler<SSTableImportRequest>
{
    private final SSTableImporter importer;
    private final SSTableUploadsPathBuilder uploadPathBuilder;
    private final Cache<SSTableImporter.ImportOptions, Future<Void>> cache;

    /**
     * Constructs a handler with the provided {@code metadataFetcher} and {@code builder} for the SSTableUploads
     * staging directory
     *
     * @param metadataFetcher   a class for fetching InstanceMetadata
     * @param importer          a class that handles importing the requests into Cassandra
     * @param uploadPathBuilder a class that provides SSTableUploads directories
     * @param cacheFactory      a factory for caches used in sidecar
     * @param executorPools     executor pools for blocking executions
     * @param validator         a validator instance to validate Cassandra-specific input
     */
    @Inject
    protected SSTableImportHandler(InstanceMetadataFetcher metadataFetcher,
                                   SSTableImporter importer,
                                   SSTableUploadsPathBuilder uploadPathBuilder,
                                   CacheFactory cacheFactory,
                                   ExecutorPools executorPools,
                                   CassandraInputValidator validator)
    {
        super(metadataFetcher, executorPools, validator);
        this.importer = importer;
        this.uploadPathBuilder = uploadPathBuilder;
        this.cache = cacheFactory.ssTableImportCache();
    }

    /**
     * Import SSTables, that have been previously uploaded, into the Cassandra service
     *
     * @param context the context for the handler
     */
    @Override
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               String host,
                               SocketAddress remoteAddress,
                               SSTableImportRequest request)
    {
        uploadPathBuilder.build(host, request)
                         .onSuccess(uploadDirectory -> {
                             SSTableImporter.ImportOptions importOptions =
                             importOptions(host, request, uploadDirectory);

                             Future<Void> importResult = cache.get(importOptions, this::importSSTablesAsync);
                             if (importResult == null)
                             {
                                 // cache is disabled
                                 importResult = importSSTablesAsync(importOptions);
                             }

                             if (!importResult.isComplete())
                             {
                                 logger.debug("ImportHandler accepted request={}, remoteAddress={}, instance={}",
                                              request, remoteAddress, host);
                                 context.response().setStatusCode(HttpResponseStatus.ACCEPTED.code()).end();
                             }
                             else if (importResult.failed())
                             {
                                 context.fail(importResult.cause());
                             }
                             else
                             {
                                 context.json(new SSTableImportResponse(true,
                                                                        request.uploadId(),
                                                                        request.keyspace(),
                                                                        request.tableName()));
                                 logger.debug("ImportHandler completed request={}, remoteAddress={}, instance={}",
                                              request, remoteAddress, host);
                             }
                         })
                         .onFailure(cause -> {
                             if (cause instanceof NoSuchFileException)
                             {
                                 logger.error("Upload directory not found for request={}, remoteAddress={}, " +
                                              "instance={}", request, remoteAddress, host, cause);
                                 context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND, cause.getMessage()));
                             }
                             else if (cause instanceof IllegalArgumentException)
                             {
                                 context.fail(wrapHttpException(HttpResponseStatus.BAD_REQUEST, cause.getMessage(),
                                                                cause));
                             }
                             else if (cause instanceof HttpException)
                             {
                                 context.fail(cause);
                             }
                             else
                             {
                                 logger.error("Unexpected error during import SSTables for request={}, " +
                                              "remoteAddress={}, instance={}", request, remoteAddress, host, cause);
                                 context.fail(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
                             }
                         });
    }

    @Override
    protected SSTableImportRequest extractParamsOrThrow(RoutingContext context)
    {
        return SSTableImportRequest.from(qualifiedTableName(context, true), context);
    }

    /**
     * Schedules the SSTable import when the Cassandra service is available.
     *
     * @param importOptions the import options
     * @return a future for the import
     */
    private Future<Void> importSSTablesAsync(SSTableImporter.ImportOptions importOptions)
    {
        CassandraAdapterDelegate cassandra = metadataFetcher.delegate(importOptions.host());
        TableOperations tableOperations = cassandra.tableOperations();

        if (tableOperations == null)
        {
            return Future.failedFuture(cassandraServiceUnavailable());
        }
        else
        {
            return uploadPathBuilder.isValidDirectory(importOptions.directory())
                                    .compose(validDirectory -> importer.scheduleImport(importOptions));
        }
    }

    private static SSTableImporter.ImportOptions importOptions(String host, SSTableImportRequest request,
                                                               String uploadDirectory)
    {
        return new SSTableImporter.ImportOptions.Builder()
               .host(host)
               .keyspace(request.keyspace())
               .tableName(request.tableName())
               .directory(uploadDirectory)
               .uploadId(request.uploadId())
               .resetLevel(request.resetLevel())
               .clearRepaired(request.clearRepaired())
               .verifySSTables(request.verifySSTables())
               .verifyTokens(request.verifyTokens())
               .invalidateCaches(request.invalidateCaches())
               .extendedVerify(request.extendedVerify())
               .copyData(request.copyData())
               .build();
    }
}
