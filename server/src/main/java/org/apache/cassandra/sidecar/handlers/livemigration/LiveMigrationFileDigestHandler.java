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

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.common.request.LiveMigrationFileDigestRequest;
import org.apache.cassandra.sidecar.common.response.DigestResponse;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.handlers.AbstractHandler;
import org.apache.cassandra.sidecar.handlers.AccessProtected;
import org.apache.cassandra.sidecar.handlers.FileStreamHandler;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.DigestAlgorithm;
import org.apache.cassandra.sidecar.utils.DigestAlgorithmFactory;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.jetbrains.annotations.NotNull;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.apache.cassandra.sidecar.common.request.LiveMigrationFileDigestRequest.DIGEST_ALGORITHM_PARAM;
import static org.apache.cassandra.sidecar.utils.AsyncFileDigestCalculator.calculateDigest;
import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * Handler for calculating and returning file digests during live migration.
 * Supported digest algorithms are determined by {@link DigestAlgorithmFactory} and specified
 * via the {@link LiveMigrationFileDigestRequest#DIGEST_ALGORITHM_PARAM} query parameter.
 */
@Singleton
public class LiveMigrationFileDigestHandler extends AbstractHandler<DigestAlgorithm> implements AccessProtected
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationFileDigestHandler.class);

    private final Vertx vertx;
    private final DigestAlgorithmFactory digestAlgorithmFactory;

    @Inject
    public LiveMigrationFileDigestHandler(InstanceMetadataFetcher metadataFetcher,
                                          ExecutorPools executorPools,
                                          CassandraInputValidator validator,
                                          Vertx vertx,
                                          DigestAlgorithmFactory digestAlgorithmFactory)
    {
        super(metadataFetcher, executorPools, validator);
        this.vertx = vertx;
        this.digestAlgorithmFactory = digestAlgorithmFactory;
    }

    @Override
    protected DigestAlgorithm extractParamsOrThrow(RoutingContext context)
    {
        String digestAlgorithmParam = getDigestAlgorithmParam(context);
        return digestAlgorithmFactory.getDigestAlgorithm(digestAlgorithmParam, 0);
    }

    @Override
    protected void handleInternal(RoutingContext context,
                                  HttpServerRequest httpRequest,
                                  @NotNull String host,
                                  SocketAddress remoteAddress,
                                  DigestAlgorithm digestAlgorithm)
    {
        String file = context.get(FileStreamHandler.FILE_PATH_CONTEXT_KEY);

        if (file == null)
        {
            LOGGER.error("File path not found in context");
            context.fail(wrapHttpException(INTERNAL_SERVER_ERROR, "File path not available"));
            return;
        }
        calculateDigest(vertx, file, digestAlgorithm)
        .onComplete(ar -> {
            if (ar.succeeded())
            {
                String digestAlgorithmParam = getDigestAlgorithmParam(context);
                DigestResponse digestResponse = new DigestResponse(ar.result(), digestAlgorithmParam);
                context.json(digestResponse);
            }
            else
            {
                LOGGER.error("Failed to calculate digest", ar.cause());
                context.fail(wrapHttpException(INTERNAL_SERVER_ERROR, ar.cause()));
            }
        });
    }

    private String getDigestAlgorithmParam(RoutingContext context)
    {
        return context.request().getParam(DIGEST_ALGORITHM_PARAM);
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Set.of(BasicPermissions.DATA_COPY.toAuthorization());
    }
}
