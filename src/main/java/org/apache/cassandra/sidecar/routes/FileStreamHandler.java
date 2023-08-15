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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.exceptions.ThrowableUtils;
import org.apache.cassandra.sidecar.models.HttpResponse;
import org.apache.cassandra.sidecar.utils.FileStreamer;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.RequestUtils;

import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Handler for sending out files.
 */
@Singleton
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
        fileSize(context, localFile)
        .compose(fileSize -> fileStreamer.stream(new HttpResponse(httpRequest, context.response()),
                                                 instanceMetadata.id(), localFile, fileSize,
                                                 httpRequest.getHeader(HttpHeaderNames.RANGE)))
        .onSuccess(v -> logger.debug("Completed streaming file '{}'", localFile))
        .onFailure(cause -> processFailure(cause, context, host, remoteAddress, localFile));
    }

    @Override
    protected String extractParamsOrThrow(RoutingContext context)
    {
        return context.get(FILE_PATH_CONTEXT_KEY);
    }

    @Override
    protected void processFailure(Throwable cause,
                                  RoutingContext context,
                                  String host,
                                  SocketAddress remoteAddress,
                                  String localFile)
    {
        IOException fileNotFoundException = ThrowableUtils.getCause(cause, NoSuchFileException.class);

        if (fileNotFoundException == null)
        {
            // FileNotFoundException comes from the stream method
            fileNotFoundException = ThrowableUtils.getCause(cause, FileNotFoundException.class);
        }

        if (fileNotFoundException != null)
        {
            logger.error("The requested file '{}' does not exist", localFile);
            context.fail(wrapHttpException(NOT_FOUND, "The requested file does not exist"));
            return;
        }

        super.processFailure(cause, context, host, remoteAddress, localFile);
    }

    protected Future<Long> fileSize(RoutingContext context, String path)
    {
        Long fileSize = RequestUtils.parseLongQueryParam(context.request(), "size", null);
        if (fileSize != null)
        {
            return Future.succeededFuture(fileSize);
        }

        return context.vertx().fileSystem().props(path)
                      .compose(fileProps -> {
                          if (fileProps == null || !fileProps.isRegularFile())
                          {
                              // File is not a regular file
                              logger.error("The requested file '{}' does not exist", path);
                              return Future.failedFuture(wrapHttpException(NOT_FOUND,
                                                                           "The requested file does not exist"));
                          }

                          if (fileProps.size() <= 0)
                          {
                              logger.error("The requested file '{}' has 0 size", path);
                              return Future.failedFuture(wrapHttpException(REQUESTED_RANGE_NOT_SATISFIABLE,
                                                                           "The requested file is empty"));
                          }

                          return Future.succeededFuture(fileProps.size());
                      });
    }
}
