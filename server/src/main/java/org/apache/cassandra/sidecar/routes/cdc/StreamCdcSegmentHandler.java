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

package org.apache.cassandra.sidecar.routes.cdc;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.cdc.CdcLogCache;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.models.HttpResponse;
import org.apache.cassandra.sidecar.routes.AbstractHandler;
import org.apache.cassandra.sidecar.routes.AccessProtected;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.CdcUtil;
import org.apache.cassandra.sidecar.utils.FileStreamer;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.utils.CdcUtil.getIdxFileName;
import static org.apache.cassandra.sidecar.utils.CdcUtil.isLogFile;
import static org.apache.cassandra.sidecar.utils.CdcUtil.isValid;
import static org.apache.cassandra.sidecar.utils.CdcUtil.parseIndexFile;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Provides REST endpoint for streaming cdc commit logs.
 */
@Singleton
public class StreamCdcSegmentHandler extends AbstractHandler<String> implements AccessProtected
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamCdcSegmentHandler.class);

    private final FileStreamer fileStreamer;
    private final CdcLogCache cdcLogCache;
    private final TaskExecutorPool serviceExecutorPool;

    @Inject
    public StreamCdcSegmentHandler(InstanceMetadataFetcher metadataFetcher,
                                   FileStreamer fileStreamer,
                                   CdcLogCache cdcLogCache,
                                   ExecutorPools executorPools,
                                   CassandraInputValidator validator)
    {
        super(metadataFetcher, executorPools, validator);
        this.fileStreamer = fileStreamer;
        this.cdcLogCache = cdcLogCache;
        this.serviceExecutorPool = executorPools.service();
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.CDC.toAuthorization());
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  @NotNull String host,
                                  SocketAddress remoteAddress,
                                  String segment)
    {
        cdcLogCache.initMaybe();
        InstanceMetadata instance = metadataFetcher.instance(host);
        String cdcDir = instance.cdcDir();
        File segmentFile = new File(cdcDir, segment);
        validateCdcSegmentFile(segmentFile);

        String indexFileName = getIdxFileName(segment);
        File indexFile = new File(cdcDir, indexFileName);
        HttpResponse response = new HttpResponse(context.request(), context.response());
        streamCdcSegmentAsync(context, segmentFile, indexFile, response, instance)
        // Touch the files at the end of the request
        // If the file exists in cache, its expiry is extended; otherwise, the cache is not changed.
        .onSuccess(res -> cdcLogCache.touch(segmentFile, indexFile))
        .onFailure(cause -> processFailure(cause, context, host, remoteAddress, segment));
    }

    private Future<Void> streamCdcSegmentAsync(RoutingContext context,
                                               File segmentFile,
                                               File indexFile,
                                               HttpResponse response,
                                               InstanceMetadata instance)
    {
        long segmentFileLength = segmentFile.length();
        return getOrCreateLinkedCdcFilePairAsync(segmentFile, indexFile)
               .compose(cdcFilePair ->
                        openCdcIndexFileAsync(cdcFilePair)
                        .compose(cdcIndex -> {
                            // stream the segment file; depending on whether the cdc segment is complete or not, cap the range of file to stream
                            String rangeHeader = context.request().getHeader(HttpHeaderNames.RANGE);
                            return cdcIndex.isCompleted
                                   ? fileStreamer.parseRangeHeader(rangeHeader, segmentFileLength)
                                   : fileStreamer.parseRangeHeader(rangeHeader, cdcIndex.latestFlushPosition);
                        })
                        .compose(range -> fileStreamer.stream(response, instance.id(), cdcFilePair.segmentFile.getAbsolutePath(), segmentFileLength, range))
               );
    }

    @Override
    protected String extractParamsOrThrow(RoutingContext context)
    {
        return context.request().getParam("segment");
    }

    private void validateCdcSegmentFile(File segmentFile) throws HttpException
    {
        // We only accept stream request for log files
        if (!isValid(segmentFile.getName()) || !isLogFile(segmentFile.getName()))
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Invalid path param for CDC segment: " + segmentFile.getName());
        }

        // check file existence
        if (!segmentFile.exists())
        {
            throw wrapHttpException(HttpResponseStatus.NOT_FOUND, "CDC segment not found: " + segmentFile.getName());
        }

        // check file content
        long fileSize = segmentFile.length();
        if (fileSize == 0)
        {
            throw wrapHttpException(HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE, "File is empty");
        }
    }

    private Future<CdcFilePair> getOrCreateLinkedCdcFilePairAsync(File segmentFile, File indexFile)
    {
        return serviceExecutorPool.executeBlocking(() -> {
            // hardlink the segment and its index file,
            // in order to guarantee the files exist in the subsequent calls (within the cache duration)
            try
            {
                File linkedSegmentFile = cdcLogCache.createLinkedFileInCache(segmentFile);
                File linkedIndexFile = cdcLogCache.createLinkedFileInCache(indexFile);
                return new CdcFilePair(linkedSegmentFile, linkedIndexFile);
            }
            catch (IOException e)
            {
                LOGGER.debug("Failed to prepare CDC segment to stream", e);
                throw wrapHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Failed to prepare CDC segment to stream", e);
            }
        });
    }

    private Future<CdcUtil.CdcIndex> openCdcIndexFileAsync(CdcFilePair cdcFilePair)
    {
        return serviceExecutorPool.executeBlocking(() -> {
            // check index file is accessible
            try
            {
                return parseIndexFile(cdcFilePair.indexFile, cdcFilePair.segmentFile.length());
            }
            catch (IOException e)
            {
                LOGGER.error("Failed to read CDC index file", e);
                throw wrapHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Failed to read CDC index file", e);
            }
        });
    }

    private static class CdcFilePair
    {
        private final File segmentFile;
        private final File indexFile;

        private CdcFilePair(File segmentFile, File indexFile)
        {
            this.segmentFile = segmentFile;
            this.indexFile = indexFile;
        }
    }
}
