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

package org.apache.cassandra.sidecar.utils;

import java.io.File;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.data.SSTableUploads;
import org.apache.cassandra.sidecar.common.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.data.SSTableUploadRequest;

/**
 * This class builds the path to the SSTable uploads staging directory
 */
@Singleton
public class SSTableUploadsPathBuilder extends BaseFileSystem
{
    // Matching ID that consists of at least 1 and at most 3 UUID-like (i.e. just the UUID chars, but not the format)
    // with '-' or ':' as the separator. 1 UUID = 36 chars, 3 UUIDs + separator = 110 chars.
    private static final Pattern UPLOAD_ID_PATTERN = Pattern.compile("[0-9a-fA-F\\-:]{36,110}");

    /**
     * Creates a new SSTableUploadsPathBuilder object with the given {@code vertx} instance.
     *
     * @param vertx           the vertx instance
     * @param instancesConfig the configuration for Cassandra
     * @param validator       a validator instance to validate Cassandra-specific input
     * @param executorPools   executor pools for blocking executions
     */
    @Inject
    public SSTableUploadsPathBuilder(Vertx vertx,
                                     InstancesConfig instancesConfig,
                                     CassandraInputValidator validator,
                                     ExecutorPools executorPools)
    {
        super(vertx.fileSystem(), instancesConfig, validator, executorPools);
    }

    /**
     * Builds the path to the SSTable uploads staging directory for the given {@code request} inside the
     * specified {@code host}.
     *
     * @param host    the name of the host
     * @param request the request
     * @param <T>     the type of the SSTableUploads
     * @return the absolute path of the SSTable uploads staging directory
     */
    public <T extends SSTableUploads> Future<String> build(String host, T request)
    {
        return validate(request)
               .compose(validRequest -> resolveUploadIdDirectory(host, request.uploadId()))
               .compose(stagingDirectory ->
                        resolveUploadDirectory(stagingDirectory, request.keyspace(), request.tableName()));
    }

    /**
     * Builds the path to the configured staging directory for the given {@code host}. Attempt to create the
     * staging directory if it doesn't exist.
     *
     * @param host the name of the host
     * @return a future to the created and validated staging directory
     */
    public Future<String> resolveStagingDirectory(String host)
    {
        InstanceMetadata instanceMeta = instancesConfig.instanceFromHost(host);
        return ensureDirectoryExists(StringUtils.removeEnd(instanceMeta.stagingDir(), File.separator));
    }

    /**
     * Builds the path to the {@code uploadId} staging directory inside the specified {@code host}.
     *
     * @param host     the name of the host
     * @param uploadId an identifier for the upload ID
     * @return the absolute path of the {@code uploadId} staging directory
     */
    public Future<String> resolveUploadIdDirectory(String host, String uploadId)
    {
        return validateUploadId(uploadId)
               .compose(validUploadId -> resolveStagingDirectory(host))
               .compose(this::isValidDirectory)
               .compose(directory -> Future.succeededFuture(directory + File.separatorChar + uploadId));
    }

    /**
     * Returns the absolute path to the upload directory where the SSTables will be uploaded for a given
     * {@code stagingDirectory}.
     *
     * @param stagingDirectory the absolute path to the staging directory
     * @param keyspace         the keyspace in Cassandra
     * @param tableName        the table name in Cassandra
     * @return the absolute path to the upload directory where the SSTables will be uploaded
     */
    protected Future<String> resolveUploadDirectory(String stagingDirectory, String keyspace, String tableName)
    {
        String uploadDirectory = StringUtils.removeEnd(stagingDirectory, File.separator)
                                 + File.separatorChar + keyspace
                                 + File.separatorChar + tableName;
        return Future.succeededFuture(uploadDirectory);
    }

    /**
     * Validates the {@code uploadId} conforms to the {@link #UPLOAD_ID_PATTERN}
     *
     * @param uploadId an identifier for the upload
     * @return a validated {@code uploadId}
     */
    protected Future<String> validateUploadId(String uploadId)
    {
        if (!UPLOAD_ID_PATTERN.matcher(uploadId).matches())
        {
            return Future.failedFuture(new IllegalArgumentException("Invalid upload id is supplied, uploadId="
                                                                    + uploadId));
        }
        return Future.succeededFuture(uploadId);
    }

    /**
     * Validates the {@link SSTableUploads request}
     *
     * @param request the request to validate
     * @param <T>     the type of the SSTableUploads
     * @return a future with the result of the validation
     */
    protected <T extends SSTableUploads> Future<T> validate(T request)
    {
        return validateUploadId(request.uploadId())
               .compose(validUploadId -> {
                   try
                   {
                       validator.validateKeyspaceName(request.keyspace());
                       validator.validateTableName(request.tableName());

                       if (request instanceof SSTableUploadRequest)
                       {
                           validator.validateComponentName(((SSTableUploadRequest) request).component());
                       }
                   }
                   catch (NullPointerException | HttpException exception)
                   {
                       return Future.failedFuture(exception);
                   }
                   return Future.succeededFuture(request);
               });
    }
}
