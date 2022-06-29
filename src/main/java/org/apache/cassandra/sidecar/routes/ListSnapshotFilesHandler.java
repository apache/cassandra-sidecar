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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.file.FileProps;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.data.ListSnapshotFilesRequest;
import org.apache.cassandra.sidecar.common.data.ListSnapshotFilesResponse;
import org.apache.cassandra.sidecar.snapshots.SnapshotPathBuilder;

import static org.apache.cassandra.sidecar.utils.RequestUtils.extractHostAddressWithoutPort;

/**
 * ListSnapshotFilesHandler class lists paths of all the snapshot files of a given snapshot name.
 * Query param includeSecondaryIndexFiles is used to request secondary index files along with other files
 * For example:
 *
 * <p>
 * /api/v1/snapshots/testSnapshot                                    lists all SSTable component files for all the
 * "testSnapshot" snapshots
 * <p>
 * /api/v1/snapshots/testSnapshot?includeSecondaryIndexFiles=true    lists all SSTable component files including
 * secondary index files for all the "testSnapshot"
 * snapshots
 * <p>
 * /api/v1/keyspace/ks/table/tbl/snapshots/testSnapshot              lists all SSTable component files for the
 * "testSnapshot" snapshot for the "ks" keyspace
 * and the "tbl" table
 * <p>
 * /api/v1/keyspace/ks/table/tbl/snapshots/testSnapshot?includeSecondaryIndexFiles=true
 * lists all SSTable component files including
 * secondary index files for the "testSnapshot"
 * snapshot for the "ks" keyspace and the "tbl"
 * table
 */
public class ListSnapshotFilesHandler extends AbstractHandler
{
    private static final Logger logger = LoggerFactory.getLogger(ListSnapshotFilesHandler.class);
    private static final String INCLUDE_SECONDARY_INDEX_FILES = "includeSecondaryIndexFiles";
    private static final int DATA_DIR_INDEX = 0;
    private final SnapshotPathBuilder builder;

    @Inject
    public ListSnapshotFilesHandler(SnapshotPathBuilder builder, InstancesConfig instancesConfig)
    {
        super(instancesConfig);
        this.builder = builder;
    }

    @Override
    public void handle(RoutingContext context)
    {
        final HttpServerRequest request = context.request();
        final String host = extractHostAddressWithoutPort(request.host());
        final SocketAddress remoteAddress = request.remoteAddress();
        final ListSnapshotFilesRequest requestParams = extractParamsOrThrow(context);
        logger.debug("ListSnapshotFilesHandler received request: {} from: {}. Instance: {}",
                     requestParams, remoteAddress, host);

        Future<List<Pair<String, FileProps>>> future;
        boolean secondaryIndexFiles = requestParams.includeSecondaryIndexFiles();
        if (requestParams.getKeyspace() != null && requestParams.getTableName() != null)
        {
            future = builder.build(host, requestParams)
                            .compose(directory -> builder.listSnapshotDirectory(directory, secondaryIndexFiles));
        }
        else
        {
            //noinspection unchecked
            future = builder.findSnapshotDirectories(host, requestParams.getSnapshotName())
                            .compose(snapshotDirectoryList -> {
                                //noinspection rawtypes
                                List<Future> futures =
                                snapshotDirectoryList.stream()
                                                     .map(directory ->
                                                          builder.listSnapshotDirectory(directory, secondaryIndexFiles))
                                                     .collect(Collectors.toList());

                                return CompositeFuture.all(futures);
                            })
                            .map(ar -> ar.list()
                                         .stream()
                                         .flatMap(l -> ((List<Pair<String, FileProps>>) l).stream())
                                         .collect(Collectors.toList()));
        }

        future.onSuccess(fileList ->
               {
                  if (fileList.isEmpty())
                  {
                      context.fail(new HttpException(HttpResponseStatus.NOT_FOUND.code(),
                                                     "Snapshot '" + requestParams.getSnapshotName() + "' not found"));
                  }
                  else
                  {
                      context.json(buildResponse(host, requestParams, fileList));
                  }
              })
              .onFailure(cause ->
              {
                  logger.error("ListSnapshotFilesHandler failed for request: {} from: {}. Instance: {}",
                               requestParams, remoteAddress, host);
                  if (cause instanceof FileNotFoundException)
                  {
                      context.fail(new HttpException(HttpResponseStatus.NOT_FOUND.code(), cause.getMessage()));
                  }
                  else
                  {
                      context.fail(new HttpException(HttpResponseStatus.BAD_REQUEST.code(), "Invalid request for "
                                                                                            + requestParams));
                  }
              });
    }

    private ListSnapshotFilesResponse buildResponse(String host,
                                                    ListSnapshotFilesRequest request,
                                                    List<Pair<String, FileProps>> fileList)
    {
        InstanceMetadata instanceMetadata = instancesConfig.instanceFromHost(host);
        int sidecarPort = instanceMetadata.port();
        Path dataDirPath = Paths.get(instanceMetadata.dataDirs().get(DATA_DIR_INDEX));
        ListSnapshotFilesResponse response = new ListSnapshotFilesResponse();
        String snapshotName = request.getSnapshotName();

        for (Pair<String, FileProps> file : fileList)
        {
            Path pathFromDataDir = dataDirPath.relativize(Paths.get(file.getLeft()));

            String keyspace = request.getKeyspace() != null
                              ? request.getKeyspace()
                              : pathFromDataDir.getName(0).toString();
            // table name might include a dash (-) with the table UUID so we always use it as part of the response
            String tableName = pathFromDataDir.getName(1).toString();
            String fileName = pathFromDataDir.subpath(4, pathFromDataDir.getNameCount()).toString();

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
