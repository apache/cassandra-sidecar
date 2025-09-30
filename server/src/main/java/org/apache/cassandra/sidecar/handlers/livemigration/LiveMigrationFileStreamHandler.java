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

package org.apache.cassandra.sidecar.handlers.livemigration;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.LiveMigrationConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.handlers.AbstractHandler;
import org.apache.cassandra.sidecar.handlers.AccessProtected;
import org.apache.cassandra.sidecar.handlers.FileStreamHandler;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationInstanceMetadataUtil;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.DIR_INDEX_PARAM;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.DIR_TYPE_PARAM;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.replacePlaceholder;

/**
 * Handler that allows Cassandra instance files to be downloaded during LiveMigration. This handler
 * doesn't stream the file but relies on {@link FileStreamHandler} to do so. This handler doesn't allow
 * using "/.." in the path to access files. This handler does not serve files which are excluded in
 * Live Migration configuration.
 */
public class LiveMigrationFileStreamHandler extends AbstractHandler<Void> implements AccessProtected
{

    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationFileStreamHandler.class);

    private final Map<Integer, List<PathMatcher>> fileExclusionsByInstanceId = new ConcurrentHashMap<>();
    private final Map<Integer, List<PathMatcher>> dirExclusionsByInstanceId = new ConcurrentHashMap<>();
    private final LiveMigrationConfiguration liveMigrationConfiguration;

    @Inject
    public LiveMigrationFileStreamHandler(InstanceMetadataFetcher metadataFetcher,
                                          ExecutorPools executorPools,
                                          CassandraInputValidator validator,
                                          SidecarConfiguration sidecarConfiguration)
    {
        super(metadataFetcher, executorPools, validator);
        this.liveMigrationConfiguration = sidecarConfiguration.liveMigrationConfiguration();
    }

    @Override
    protected Void extractParamsOrThrow(RoutingContext context)
    {
        String dirType = context.pathParam(DIR_TYPE_PARAM);
        if (null == dirType || dirType.isEmpty() || null == LiveMigrationDirType.find(dirType))
        {
            throw new HttpException(HttpResponseStatus.BAD_REQUEST.code(), "Invalid directory type: " + dirType);
        }

        String dirIndex = context.pathParam(DIR_INDEX_PARAM);
        int index;
        try
        {
            index = Integer.parseInt(dirIndex);
        }
        catch (NumberFormatException formatException)
        {
            throw new HttpException(HttpResponseStatus.BAD_REQUEST.code(), "Invalid directoryIndex: " + dirIndex);
        }
        if (index < 0)
        {
            throw new HttpException(HttpResponseStatus.BAD_REQUEST.code(), "Invalid directoryIndex: " + dirIndex);
        }

        // Path params are not used further, hence returning null.
        return null;
    }

    @Override
    protected void handleInternal(RoutingContext rc,
                                  HttpServerRequest httpRequest,
                                  @NotNull String host,
                                  SocketAddress remoteAddress,
                                  @Nullable Void request)
    {
        String reqPath = URLDecoder.decode(rc.request().path(), StandardCharsets.UTF_8);

        if (reqPath.contains("/../") || reqPath.endsWith("/.."))
        {
            LOGGER.warn("Tried to access file using relative path({}). Rejecting the request.", reqPath);
            rc.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end();
            return;
        }

        InstanceMetadata instanceMeta = metadataFetcher.instance(host);
        String normalizedPath = rc.normalizedPath();
        String localFile;

        try
        {
            localFile = LiveMigrationInstanceMetadataUtil.localPath(normalizedPath, instanceMeta).toString();
        }
        catch (IllegalArgumentException e)
        {
            LOGGER.warn("Invalid path", e);
            rc.response().setStatusCode(HttpResponseStatus.NOT_FOUND.code()).end();
            return;
        }

        Path path = Paths.get(localFile);

        executorPools.service()
                     .executeBlocking(() -> isInvalidPath(rc, path, reqPath, instanceMeta))
                     .onSuccess(invalid -> {
                         if (!invalid)
                         {
                             rc.put(FileStreamHandler.FILE_PATH_CONTEXT_KEY, localFile);
                             rc.next();
                         }
                     });
    }

    private boolean isInvalidPath(RoutingContext rc, Path path, String reqPath, InstanceMetadata instanceMeta)
    {
        if (!Files.exists(path))
        {
            LOGGER.info("Requested file is not found. file={}", path);
            rc.response().setStatusCode(HttpResponseStatus.NOT_FOUND.code()).end();
            return true;
        }
        if (Files.isDirectory(path))
        {
            LOGGER.info("Cannot transfer directory. path={}.", reqPath);
            rc.response().setStatusCode(HttpResponseStatus.BAD_REQUEST.code()).end();
            return true;
        }
        if (isExcluded(path, instanceMeta))
        {
            LOGGER.debug("Requested path or one of its parent directories is excluded from Live Migration. " +
                         "path={}", reqPath);
            rc.response().setStatusCode(HttpResponseStatus.NOT_FOUND.code()).end();
            return true;
        }
        return false;
    }

    private boolean isExcluded(Path localFile, InstanceMetadata instanceMetadata)
    {
        return isFileExcluded(localFile, instanceMetadata) || isDirExcluded(localFile.getParent(), instanceMetadata);
    }

    private boolean isFileExcluded(Path localFile, InstanceMetadata instanceMetadata)
    {
        List<PathMatcher> fileExclusionMatchers = getPathMatchers(fileExclusionsByInstanceId,
                                                                  instanceMetadata,
                                                                  LiveMigrationConfiguration::filesToExclude);

        return isMatch(localFile, fileExclusionMatchers);
    }

    private boolean isDirExcluded(Path dir, InstanceMetadata instanceMetadata)
    {
        List<PathMatcher> dirExclusionMatchers = getPathMatchers(dirExclusionsByInstanceId,
                                                                 instanceMetadata,
                                                                 LiveMigrationConfiguration::directoriesToExclude);

        // Recursively check all parent directories to see if they are excluded or not.
        while (dir != null)
        {
            if (isMatch(dir, dirExclusionMatchers))
            {
                return true;
            }
            dir = dir.getParent();
        }

        return false;
    }

    private List<PathMatcher> getPathMatchers(Map<Integer, List<PathMatcher>> map,
                                              InstanceMetadata instanceMetadata,
                                              Function<LiveMigrationConfiguration, Set<String>> exclusionProvider)
    {
        return map.computeIfAbsent(instanceMetadata.id(), id -> {
            Set<String> exclusions = exclusionProvider.apply(liveMigrationConfiguration);
            List<PathMatcher> matchers = new ArrayList<>(exclusions.size());
            for (String placeholderPattern : exclusions)
            {
                Set<String> filePatterns = replacePlaceholder(placeholderPattern, instanceMetadata);
                filePatterns.forEach(filePattern -> matchers.add(FileSystems.getDefault().getPathMatcher(filePattern)));
            }
            return matchers;
        });
    }

    private boolean isMatch(Path localFile, List<PathMatcher> pathMatchers)
    {
        if (null == pathMatchers || pathMatchers.isEmpty())
        {
            return false;
        }

        for (PathMatcher pathMatcher : pathMatchers)
        {
            if (pathMatcher.matches(localFile))
            {
                LOGGER.debug("Requested file is excluded from Live Migration. file={}", localFile);
                return true;
            }
        }
        return false;
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Set.of(BasicPermissions.STREAM_FILES.toAuthorization());
    }
}
