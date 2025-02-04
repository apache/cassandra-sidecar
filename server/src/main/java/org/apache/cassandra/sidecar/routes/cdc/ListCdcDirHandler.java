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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.acl.authorization.VariableAwareResource;
import org.apache.cassandra.sidecar.common.response.ListCdcSegmentsResponse;
import org.apache.cassandra.sidecar.common.response.data.CdcSegmentInfo;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.routes.AbstractHandler;
import org.apache.cassandra.sidecar.routes.AccessProtected;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.CdcUtil;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.sidecar.utils.CdcUtil.getIdxFileName;
import static org.apache.cassandra.sidecar.utils.CdcUtil.getLogFilePrefix;
import static org.apache.cassandra.sidecar.utils.CdcUtil.isIndexFile;
import static org.apache.cassandra.sidecar.utils.CdcUtil.parseIndexFile;

/**
 * Provides REST endpoint for listing commit logs in CDC directory.
 */
@Singleton
public class ListCdcDirHandler extends AbstractHandler<Void> implements AccessProtected
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ListCdcDirHandler.class);
    private final ServiceConfiguration config;
    private final TaskExecutorPool serviceExecutorPool;

    @Inject
    public ListCdcDirHandler(InstanceMetadataFetcher metadataFetcher,
                             SidecarConfiguration config,
                             ExecutorPools executorPools,
                             CassandraInputValidator validator)
    {
        super(metadataFetcher, executorPools, validator);
        this.config = config.serviceConfiguration();
        this.serviceExecutorPool = executorPools.service();
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        String resource = VariableAwareResource.CLUSTER.resource();
        return Collections.singleton(BasicPermissions.CDC.toAuthorization(resource));
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  @NotNull String host,
                                  SocketAddress remoteAddress,
                                  Void request)
    {
        String cdcDir = metadataFetcher.instance(host).cdcDir();
        serviceExecutorPool
        .executeBlocking(() -> collectCdcSegmentsFromFileSystem(cdcDir))
        .map(segments -> new ListCdcSegmentsResponse(config.host(), config.port(), segments))
        .onSuccess(context::json)
        .onFailure(cause -> {
            LOGGER.warn("Error listing the CDC commit log segments", cause);
            context.response()
                   .setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code())
                   .setStatusMessage(cause.getMessage())
                   .end();
        });
    }

    private List<CdcSegmentInfo> collectCdcSegmentsFromFileSystem(String cdcDirPath) throws IOException
    {
        List<CdcSegmentInfo> segmentInfos = new ArrayList<>();
        File cdcDir = Paths.get(cdcDirPath).toAbsolutePath().toFile();
        if (!cdcDir.isDirectory())
        {
            throw new IOException("CDC directory does not exist");
        }

        File[] cdcFiles = cdcDir.listFiles();
        if (cdcFiles == null || cdcFiles.length == 0)
        {
            return segmentInfos;
        }

        Set<String> idxFileNamePrefixes = new HashSet<>();
        for (File cdcFile : cdcFiles)
        {
            if (CdcUtil.matchIndexExtension(cdcFile.getName()))
            {
                idxFileNamePrefixes.add(CdcUtil.getIdxFilePrefix(cdcFile.getName()));
            }
        }

        for (File cdcFile : cdcFiles)
        {
            String fileName = cdcFile.getName();
            BasicFileAttributes fileAttributes = Files.readAttributes(cdcFile.toPath(), BasicFileAttributes.class);
            if (!cdcFile.exists()
                || !fileAttributes.isRegularFile() // the file just gets deleted? ignore it
                || isIndexFile(fileName) // ignore all .idx files
                || !idxFileNamePrefixes.contains(getLogFilePrefix(fileName))) // ignore .log files found without matching .idx files
            {
                continue;
            }

            CdcUtil.CdcIndex cdcIndex = parseIndexFile(new File(cdcDirPath, getIdxFileName(fileName)), fileAttributes.size());
            CdcSegmentInfo segmentInfo =
            new CdcSegmentInfo(fileName, fileAttributes.size(),
                               cdcIndex.latestFlushPosition, cdcIndex.isCompleted,
                               fileAttributes.lastModifiedTime().toMillis());
            segmentInfos.add(segmentInfo);
        }
        return segmentInfos;
    }

    @Override
    protected Void extractParamsOrThrow(RoutingContext context)
    {
        return null;
    }
}
