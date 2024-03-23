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
import io.netty.handler.codec.http.HttpHeaderNames;
import io.vertx.core.Future;
import io.vertx.core.file.FileProps;
import io.vertx.core.file.FileSystem;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.models.HttpResponse;
import org.apache.cassandra.sidecar.utils.FileStreamer;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Handler for sending out files.
 */
public class FileStreamHandler extends AbstractHandler<String>
{
    public static final String FILE_PATH_CONTEXT_KEY = "fileToTransfer";
    private final FileStreamer fileStreamer;

    @Inject
    public FileStreamHandler(InstanceMetadataFetcher metadataFetcher,
                             FileStreamer fileStreamer,
                             ExecutorPools executorPools)
    {
        super(metadataFetcher, executorPools, null);
        this.fileStreamer = fileStreamer;
    }

    @Override
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               String host,
                               SocketAddress remoteAddress,
                               String localFile)
    {
        InstanceMetadata instanceMetadata = metadataFetcher.instance(host);
        FileSystem fs = context.vertx().fileSystem();
        fs.exists(localFile)
          .compose(exists -> ensureValidFile(fs, localFile, exists))
          .compose(fileProps -> fileStreamer.stream(new HttpResponse(httpRequest, context.response()),
                                                    instanceMetadata.id(), localFile, fileProps.size(),
                                                    httpRequest.getHeader(HttpHeaderNames.RANGE)))
          .onSuccess(v -> logger.debug("Completed streaming file '{}'", localFile))
          .onFailure(context::fail);
    }

    @Override
    protected String extractParamsOrThrow(RoutingContext context)
    {
        return context.get(FILE_PATH_CONTEXT_KEY);
    }

    /**
     * Ensures that the file exists and is a non-empty regular file
     *
     * @param fs        The underlying filesystem
     * @param localFile The path the file in the filesystem
     * @param exists    Whether the file exists or not
     * @return a succeeded future with the {@link FileProps}, or a failed future if the file does not exist;
     * is not a regular file; or if the file is empty
     */
    private Future<FileProps> ensureValidFile(FileSystem fs, String localFile, Boolean exists)
    {
        if (!exists)
        {
            logger.error("The requested file '{}' does not exist", localFile);
            return Future.failedFuture(wrapHttpException(NOT_FOUND, "The requested file does not exist"));
        }

        return fs.props(localFile)
                 .compose(fileProps -> {
                     if (fileProps == null || !fileProps.isRegularFile())
                     {
                         // File is not a regular file
                         logger.error("The requested file '{}' does not exist", localFile);
                         return Future.failedFuture(wrapHttpException(NOT_FOUND, "The requested file does not exist"));
                     }

                     if (fileProps.size() <= 0)
                     {
                         logger.error("The requested file '{}' has 0 size", localFile);
                         return Future.failedFuture(wrapHttpException(REQUESTED_RANGE_NOT_SATISFIABLE,
                                                                      "The requested file is empty"));
                     }

                     return Future.succeededFuture(fileProps);
                 });
    }
}
