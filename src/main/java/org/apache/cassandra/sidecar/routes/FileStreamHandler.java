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

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.vertx.core.Future;
import io.vertx.core.file.FileProps;
import io.vertx.core.file.FileSystem;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.cache.ToggleableCache;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.models.HttpResponse;
import org.apache.cassandra.sidecar.utils.FileStreamer;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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

    /**
     * The file props cache maintains a cache of {@link FileProps} of recently streamed files.
     * This cache avoids having to validate file properties during bulk reads. In the case of
     * bulk reads, sub-ranges of an SSTable component are streamed, therefore reading the file
     * properties can occur multiple times during bulk reads for the same file. This cache aims
     * to reduce filesystem access to resolve the file props used for file streaming.
     */
    @Nullable
    private final Cache<String, Future<FileProps>> filePropsCache;

    @Inject
    public FileStreamHandler(InstanceMetadataFetcher metadataFetcher,
                             ServiceConfiguration serviceConfiguration,
                             FileStreamer fileStreamer,
                             ExecutorPools executorPools)
    {
        super(metadataFetcher, executorPools, null);
        this.fileStreamer = fileStreamer;
        this.filePropsCache = initializeCache(serviceConfiguration.fileStreamPropsCache());
    }

    @Override
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               String host,
                               SocketAddress remoteAddress,
                               String localFile)
    {
        FileSystem fs = context.vertx().fileSystem();
        ensureValidFile(fs, localFile)
        .compose(fileProps -> fileStreamer.stream(new HttpResponse(httpRequest, context.response()), localFile,
                                                  fileProps.size(), httpRequest.getHeader(HttpHeaderNames.RANGE)))
        .onSuccess(v -> logger.debug("Completed streaming file '{}'", localFile))
        .onFailure(cause -> processFailure(cause, context, host, remoteAddress, localFile));
    }

    @Override
    protected String extractParamsOrThrow(RoutingContext context)
    {
        return context.get(FILE_PATH_CONTEXT_KEY);
    }

    /**
     * Retrieves the Future for the validation operation. If the cache was initialized, it will retrieve the
     * Future from the cache.
     *
     * @param fs        The underlying filesystem
     * @param localFile The path the file in the filesystem
     * @return a succeeded future with the {@link FileProps}, or a failed future if the file does not exist;
     * is not a regular file; or if the file is empty
     */
    protected Future<FileProps> ensureValidFile(FileSystem fs, String localFile)
    {
        return filePropsCache != null
               ? filePropsCache.get(localFile, ensureValidFileNonCached(fs))
               : ensureValidFileNonCached(fs).apply(localFile);
    }

    /**
     * Ensures that the file exists and is a non-empty regular file
     *
     * @param fs The underlying filesystem
     * @return a succeeded future with the {@link FileProps}, or a failed future if the file does not exist;
     * is not a regular file; or if the file is empty
     */
    @NotNull
    protected Function<String, Future<FileProps>> ensureValidFileNonCached(FileSystem fs)
    {
        return path -> fs.props(path)
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

                             return Future.succeededFuture(fileProps);
                         });
    }

    /**
     * Initializes the cache with the provided configuration. If the configuration is null, the cache will
     * not be initialized.
     *
     * @param config the configuration for the cache
     * @return the new cache object
     */
    @Nullable
    protected Cache<String, Future<FileProps>> initializeCache(@Nullable CacheConfiguration config)
    {
        if (config == null)
        {
            logger.info("No configuration provided for filePropsCache. Skipping initialization.");
            return null;
        }
        Cache<String, Future<FileProps>> delegate =
        Caffeine.newBuilder()
                .maximumSize(config.maximumSize())
                .expireAfterAccess(config.expireAfterAccessMillis(), TimeUnit.MILLISECONDS)
                .recordStats()
                .removalListener((key, value, cause) ->
                                 logger.debug("Removed from cache=filePropsCache, entry={}, key={}, cause={}",
                                              value, key, cause))
                .build();
        return new ToggleableCache<>(delegate, config::enabled);
    }
}
