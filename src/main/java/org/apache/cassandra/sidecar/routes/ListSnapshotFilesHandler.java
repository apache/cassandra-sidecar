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
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.file.FileProps;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.Configuration;
import org.apache.cassandra.sidecar.common.data.ListSnapshotFilesRequest;
import org.apache.cassandra.sidecar.common.data.ListSnapshotFilesResponse;
import org.apache.cassandra.sidecar.snapshots.SnapshotPathBuilder;

import static org.apache.cassandra.sidecar.snapshots.SnapshotPathBuilder.SNAPSHOTS_DIR_NAME;

/**
 * ListSnapshotFilesHandler class lists paths of all the snapshot files of a given snapshot name.
 * Query param includeSecondaryIndexFiles is used to request secondary index files along with other files
 * For example:
 *
 * <p>
 * /api/v1/keyspace/ks/table/tbl/snapshots/testSnapshot
 * lists all SSTable component files for the "testSnapshot" snapshot for the "ks" keyspace and the "tbl" table
 * <p>
 * /api/v1/keyspace/ks/table/tbl/snapshots/testSnapshot?includeSecondaryIndexFiles=true
 * lists all SSTable component files including secondary index files for the "testSnapshot" snapshot for the "ks"
 * keyspace and the "tbl" table
 */
public class ListSnapshotFilesHandler extends AbstractHandler
{
    private static final Logger logger = LoggerFactory.getLogger(ListSnapshotFilesHandler.class);
    private static final String INCLUDE_SECONDARY_INDEX_FILES = "includeSecondaryIndexFiles";
    private static final int DATA_DIR_INDEX = 0;
    private static final int TABLE_NAME_SUBPATH_INDEX_FROM_END = 4;
    private static final int TABLE_NAME_SUBPATH_INDEX_FROM_END_SECONDARY = 5;
    private static final int FILE_NAME_SUBPATH_INDEX_FROM_END = 1;
    private final SnapshotPathBuilder builder;
    private final Configuration configuration;

    @Inject
    public ListSnapshotFilesHandler(SnapshotPathBuilder builder, Configuration configuration)
    {
        super(configuration.getInstancesConfig());
        this.builder = builder;
        this.configuration = configuration;
    }

    @Override
    public void handle(RoutingContext context)
    {
        final HttpServerRequest request = context.request();
        final String host = getHost(context);
        final SocketAddress remoteAddress = request.remoteAddress();
        final ListSnapshotFilesRequest requestParams = extractParamsOrThrow(context);
        logger.debug("ListSnapshotFilesHandler received request: {} from: {}. Instance: {}",
                     requestParams, remoteAddress, host);

        boolean secondaryIndexFiles = requestParams.includeSecondaryIndexFiles();

        builder.build(host, requestParams)
               .compose(directory -> builder.listSnapshotDirectory(directory, secondaryIndexFiles))
               .onSuccess(fileList ->
                          {
                              if (fileList.isEmpty())
                              {
                                  String payload = "Snapshot '" + requestParams.getSnapshotName() + "' not found";
                                  context.fail(new HttpException(HttpResponseStatus.NOT_FOUND.code(), payload));
                              }
                              else
                              {
                                  logger.debug("ListSnapshotFilesHandler handled {} for {}. Instance: {}",
                                               requestParams, remoteAddress, host);
                                  context.json(buildResponse(host, requestParams, fileList));
                              }
                          })
               .onFailure(cause ->
                          {
                              logger.error("ListSnapshotFilesHandler failed for request: {} from: {}. Instance: {}",
                                           requestParams, remoteAddress, host);
                              if (cause instanceof FileNotFoundException ||
                                  cause instanceof NoSuchFileException)
                              {
                                  context.fail(new HttpException(HttpResponseStatus.NOT_FOUND.code(),
                                                                 cause.getMessage()));
                              }
                              else
                              {
                                  context.fail(new HttpException(HttpResponseStatus.BAD_REQUEST.code(),
                                                                 "Invalid request for " + requestParams));
                              }
                          });
    }

    private ListSnapshotFilesResponse buildResponse(String host,
                                                    ListSnapshotFilesRequest request,
                                                    List<Pair<String, FileProps>> fileList)
    {
        ListSnapshotFilesResponse response = new ListSnapshotFilesResponse();
        String snapshotName = request.getSnapshotName();
        int sidecarPort = configuration.getPort();

        for (Pair<String, FileProps> file : fileList)
        {
            Path path = Paths.get(file.getLeft());
            int nameCount = path.getNameCount();

            String keyspace = request.getKeyspace();
            // table name might include a dash (-) with the table UUID, so we always use it as part of the response
            String tableName;
            if (isSecondaryIndexFile(path, keyspace))
            {
                tableName = path.getName(nameCount - TABLE_NAME_SUBPATH_INDEX_FROM_END_SECONDARY)
                                .toString();
            }
            else
            {
                tableName = path.getName(nameCount - TABLE_NAME_SUBPATH_INDEX_FROM_END)
                                .toString();
            }
            String fileName = path.getName(nameCount - FILE_NAME_SUBPATH_INDEX_FROM_END).toString();

            response.addSnapshotFile(new ListSnapshotFilesResponse.FileInfo(file.getRight().size(),
                                                                            host,
                                                                            sidecarPort,
                                                                            DATA_DIR_INDEX,
                                                                            snapshotName,
                                                                            keyspace,
                                                                            tableName,
                                                                            fileName));
        }
        return response;
    }

    /**
     * Checks whether the given path is a secondary index file by checking the parent directory "/snapshots/" as
     * well as the provided "/&lt;{@code keyspace}&gt;/".
     *
     * @param path     the path to check
     * @param keyspace the name of the keyspace
     * @return true if the path is a secondary index file, false otherwise
     */
    private boolean isSecondaryIndexFile(Path path, String keyspace)
    {
        return path.getNameCount() >= 6 &&
               path.getName(path.getNameCount() - 6).endsWith(keyspace) &&
               path.getName(path.getNameCount() - 4).endsWith(SNAPSHOTS_DIR_NAME);
    }

    private ListSnapshotFilesRequest extractParamsOrThrow(final RoutingContext context)
    {
        boolean includeSecondaryIndexFiles =
        "true".equalsIgnoreCase(context.request().getParam(INCLUDE_SECONDARY_INDEX_FILES, "false"));

        return new ListSnapshotFilesRequest(context.pathParam("keyspace"),
                                            context.pathParam("table"),
                                            context.pathParam("snapshot"),
                                            includeSecondaryIndexFiles
        );
    }
}
