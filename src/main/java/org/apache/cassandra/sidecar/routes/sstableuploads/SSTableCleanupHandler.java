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

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.routes.AbstractHandler;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.SSTableUploadsPathBuilder;

/**
 * Manages cleaning up uploaded SSTables
 */
public class SSTableCleanupHandler extends AbstractHandler<String>
{
    private static final String UPLOAD_ID_PARAM = "uploadId";
    private final SSTableUploadsPathBuilder uploadPathBuilder;

    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher   the instance metadata fetcher
     * @param uploadPathBuilder a class that provides SSTableUploads directories
     * @param executorPools     executor pools for blocking executions
     */
    @Inject
    protected SSTableCleanupHandler(InstanceMetadataFetcher metadataFetcher,
                                    SSTableUploadsPathBuilder uploadPathBuilder,
                                    ExecutorPools executorPools)
    {
        super(metadataFetcher, executorPools, null);
        this.uploadPathBuilder = uploadPathBuilder;
    }

    /**
     * Handles cleaning up the SSTable upload staging directory
     *
     * @param context the context for the handler
     */
    @Override
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               String host,
                               SocketAddress remoteAddress,
                               String uploadId)
    {
        uploadPathBuilder.resolveStagingDirectory(host, uploadId)
                         .compose(uploadPathBuilder::isValidDirectory)
                         .compose(stagingDirectory -> context.vertx()
                                                             .fileSystem()
                                                             .deleteRecursive(stagingDirectory, true))
                         .onSuccess(x -> context.response().end())
                         .onFailure(cause -> {
                             if (cause instanceof NoSuchFileException)
                             {
                                 logger.warn("Upload directory not found. uploadId={}, remoteAddress={}, instance={}",
                                             uploadId, remoteAddress, host, cause);
                                 context.fail(HttpResponseStatus.NOT_FOUND.code());
                             }
                             else if (cause instanceof IllegalArgumentException)
                             {
                                 context.fail(HttpResponseStatus.BAD_REQUEST.code(), cause);
                             }
                             else
                             {
                                 logger.error("Unable to cleanup upload. uploadId={}, remoteAddress={}, instance={}",
                                              uploadId, remoteAddress, host, cause);
                                 context.fail(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
                             }
                         });
    }

    @Override
    protected String extractParamsOrThrow(RoutingContext context)
    {
        // The route registration guarantees the uploadId path exist
        return context.pathParam(UPLOAD_ID_PARAM);
    }
}
