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

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

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
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.LiveMigrationConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.exceptions.LiveMigrationExceptions.UnknownMigrationPrefixException;
import org.apache.cassandra.sidecar.handlers.AbstractHandler;
import org.apache.cassandra.sidecar.handlers.AccessProtected;
import org.apache.cassandra.sidecar.handlers.FileStreamHandler;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationInstanceMetadataUtil;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationInstanceMetadataUtil.ResolvedPath;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.DIR_INDEX_PARAM;
import static org.apache.cassandra.sidecar.common.ApiEndpointsV1.DIR_TYPE_PARAM;
import static org.apache.cassandra.sidecar.livemigration.LiveMigrationPlaceholderUtil.replacePlaceholder;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Handler that resolves and validates file paths for live migration operations.
 * This handler validates that the requested file exists, is accessible, and is not excluded
 * from live migration. It sets the resolved file path in the routing context for downstream handlers.
 * This handler does not allow using "/.." in the path to access files and does not serve files
 * which are excluded in the Live Migration configuration.
 */
@Singleton
public class LiveMigrationFileResolveHandler extends AbstractHandler<Void> implements AccessProtected
{

    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationFileResolveHandler.class);

    private final Map<Integer, List<PathMatcher>> fileExclusionsByInstanceId = new ConcurrentHashMap<>();
    private final Map<Integer, List<PathMatcher>> dirExclusionsByInstanceId = new ConcurrentHashMap<>();
    private final LiveMigrationConfiguration liveMigrationConfiguration;

    @Inject
    public LiveMigrationFileResolveHandler(InstanceMetadataFetcher metadataFetcher,
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
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Invalid directory type: " + dirType);
        }

        String dirIndex = context.pathParam(DIR_INDEX_PARAM);
        int index;
        try
        {
            index = Integer.parseInt(dirIndex);
        }
        catch (NumberFormatException formatException)
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Invalid directoryIndex: " + dirIndex);
        }
        if (index < 0)
        {
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Invalid directoryIndex: " + dirIndex);
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
        String reqPath;
        try
        {
            reqPath = URI.create(rc.request().path()).getPath();
        }
        catch (IllegalArgumentException e)
        {
            rc.fail(wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Malformed request path", e));
            return;
        }

        if (reqPath.contains("/../") || reqPath.endsWith("/.."))
        {
            LOGGER.warn("Tried to access file using relative path({}). Rejecting the request.", reqPath);
            rc.fail(wrapHttpException(HttpResponseStatus.BAD_REQUEST,
                                      "Tried to access file using relative path: " + reqPath));
            return;
        }

        InstanceMetadata instanceMeta = metadataFetcher.instance(host);
        String normalizedPath = rc.normalizedPath();

        ResolvedPath resolved = LiveMigrationInstanceMetadataUtil.resolveLexically(normalizedPath, instanceMeta);

        // Only the filesystem-touching checks (verifyContainment, isDirectory, isExcluded) run on
        // the worker thread; the lexical resolve above is pure string work and stays on the event
        // loop. validate() throws HttpException on any failure, which flows through processFailure
        // -> context.fail() so the framework's failure handler renders a JSON error response.
        String localFile = resolved.resolvedPath().toString();
        executorPools.service()
                     .executeBlocking(() -> {
                         validate(resolved, instanceMeta);
                         return localFile;
                     })
                     .onSuccess(file -> {
                         rc.put(FileStreamHandler.FILE_PATH_CONTEXT_KEY, file);
                         rc.next();
                     })
                     .onFailure(cause -> processFailure(cause, rc, host, remoteAddress, request));
    }

    @Override
    protected void processFailure(Throwable cause, RoutingContext context, String host, SocketAddress remoteAddress, Void request)
    {
        if (cause instanceof UnknownMigrationPrefixException)
        {
            context.fail(wrapHttpException(HttpResponseStatus.NOT_FOUND, cause.getMessage(), cause));
        }
        else
        {
            super.processFailure(cause, context, host, remoteAddress, request);
        }
    }

    private void validate(ResolvedPath resolved, InstanceMetadata instanceMeta)
    {
        Path path = resolved.resolvedPath();
        try
        {
            resolved.verifyContainment();
        }
        catch (NoSuchFileException e)
        {
            LOGGER.info("Requested file is not found. file={}", path);
            throw wrapHttpException(HttpResponseStatus.NOT_FOUND, "File not found", e);
        }
        catch (IllegalArgumentException e)
        {
            throw wrapHttpException(HttpResponseStatus.FORBIDDEN, e.getMessage(), e);
        }
        catch (IOException e)
        {
            LOGGER.error("Filesystem error while resolving {}", path, e);
            throw wrapHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                    "Filesystem error while resolving requested path", e);
        }

        if (Files.isDirectory(path))
        {
            LOGGER.info("Cannot transfer directory. path={}", path);
            throw wrapHttpException(HttpResponseStatus.BAD_REQUEST, "Cannot transfer directory");
        }
        if (isExcluded(path, instanceMeta))
        {
            LOGGER.debug("Requested path or one of its parent directories is excluded from Live Migration. " +
                         "path={}", path);
            throw wrapHttpException(HttpResponseStatus.NOT_FOUND,
                                    "Requested path is excluded from live migration");
        }
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
        return Set.of(BasicPermissions.DATA_COPY.toAuthorization());
    }
}
