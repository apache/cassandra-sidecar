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

package org.apache.cassandra.sidecar.livemigration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.response.LiveMigrationStatus;
import org.apache.cassandra.sidecar.common.response.LiveMigrationStatus.MigrationState;

import static org.apache.cassandra.sidecar.common.response.LiveMigrationStatus.NOT_COMPLETED_STATUS;

/**
 * Implementation of LiveMigrationStatusTracker that manages migration status using files.
 * Tracks completion state by creating/reading JSON files in the staging directory.
 * Uses per-instance locks to ensure thread-safe file operations.
 */
@Singleton
public class LiveMigrationStatusTrackerImpl implements LiveMigrationStatusTracker
{

    @VisibleForTesting
    static final String STATUS_FILE_NAME = "live_migration_status.json";

    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationStatusTrackerImpl.class);
    private final Vertx vertx;
    private final ObjectMapper objectMapper;

    // Per-instance locks to ensure atomicity of file operations
    private final ConcurrentHashMap<String, Object> instanceLocks = new ConcurrentHashMap<>();

    @Inject
    public LiveMigrationStatusTrackerImpl(Vertx vertx)
    {
        this.vertx = vertx;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Gets the lock object for a specific instance to ensure thread-safe operations.
     *
     * @param host the instance host
     * @return lock object for the instance
     */
    private Object getLockForInstance(String host)
    {
        return instanceLocks.computeIfAbsent(host, k -> new Object());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<Void> setMigrationStatus(InstanceMetadata instanceMetadata,
                                           LiveMigrationStatus status)
    {
        Promise<Void> promise = Promise.promise();

        vertx.executeBlocking(() -> {
            synchronized (getLockForInstance(instanceMetadata.host()))
            {
                try
                {
                    Path stagingDir = ensureStagingDir(instanceMetadata);
                    Path statusFilePath = stagingDir.resolve(STATUS_FILE_NAME);

                    String content = objectMapper.writeValueAsString(status);
                    // Files should be created only once. If attempted to create file one more time,
                    // then something is wrong.
                    Files.write(statusFilePath, content.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE_NEW);

                    LOGGER.info("Live migration file for instance {} successfully created at {}.",
                                instanceMetadata.host(), statusFilePath.toAbsolutePath());
                    promise.complete();
                    return null;
                }
                catch (JsonProcessingException e)
                {
                    LOGGER.error("Failed to serialize LiveMigrationStatus to JSON string. Status={}", status, e);
                    promise.fail(e);
                    return null;
                }
                catch (IOException e)
                {
                    LOGGER.error("Failed to save LiveMigrationStatus. Status={}", status, e);
                    promise.fail(e);
                    return null;
                }
            }
        }, false);

        return promise.future();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<LiveMigrationStatus> getMigrationStatus(InstanceMetadata instanceMetadata)
    {
        Promise<LiveMigrationStatus> promise = Promise.promise();

        vertx.executeBlocking(() -> {
            synchronized (getLockForInstance(instanceMetadata.host()))
            {
                try
                {
                    Path stagingDir = ensureStagingDir(instanceMetadata);
                    Path statusFile = stagingDir.resolve(STATUS_FILE_NAME);

                    if (!Files.exists(statusFile))
                    {
                        promise.complete(NOT_COMPLETED_STATUS);
                        return null;
                    }

                    String content = Files.readString(statusFile, StandardCharsets.UTF_8);
                    LiveMigrationStatus status = objectMapper.readValue(content, LiveMigrationStatus.class);
                    promise.complete(status);
                    return null;
                }
                catch (IOException e)
                {
                    LOGGER.error("Failed to read LiveMigrationStatus for instance {}", instanceMetadata.host(), e);
                    promise.fail(e);
                    return null;
                }
            }
        }, false);

        return promise.future();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<Boolean> hasMigrationCompleted(InstanceMetadata instanceMetadata)
    {
        return getMigrationStatus(instanceMetadata)
               .compose(status -> Future.succeededFuture(status.state() == MigrationState.COMPLETED));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<Void> clearMigrationStatus(InstanceMetadata instanceMetadata)
    {
        Promise<Void> promise = Promise.promise();

        vertx.executeBlocking(() -> {
            synchronized (getLockForInstance(instanceMetadata.host()))
            {
                try
                {
                    Path stagingDir = ensureStagingDir(instanceMetadata);
                    Path statusFile = stagingDir.resolve(STATUS_FILE_NAME);

                    if (!Files.exists(statusFile))
                    {
                        promise.fail(new IllegalArgumentException(
                        "Live Migration status file does not exist for instance " + instanceMetadata.host()));
                        return null;
                    }

                    // Read and verify status
                    String content = Files.readString(statusFile, StandardCharsets.UTF_8);
                    LiveMigrationStatus status = objectMapper.readValue(content, LiveMigrationStatus.class);

                    if (status.state() != MigrationState.COMPLETED)
                    {
                        promise.fail(new IllegalArgumentException(
                        "Live Migration status is not set as completed for instance " + instanceMetadata.host()));
                        return null;
                    }

                    // Delete the file
                    Files.delete(statusFile);
                    LOGGER.info("Live migration status file deleted for instance {}", instanceMetadata.host());
                    promise.complete();
                    return null;
                }
                catch (IOException e)
                {
                    LOGGER.error("Failed to clear LiveMigrationStatus for instance {}", instanceMetadata.host(), e);
                    promise.fail(e);
                    return null;
                }
            }
        }, false);

        return promise.future();
    }

    /**
     * Ensures the staging directory exists for the given instance.
     *
     * @param instanceMetadata the instance metadata
     * @return the staging directory path
     * @throws IOException              if directory creation fails
     * @throws IllegalArgumentException if staging directory is not configured
     */
    private Path ensureStagingDir(InstanceMetadata instanceMetadata) throws IOException
    {
        if (instanceMetadata.stagingDir() == null)
        {
            throw new IllegalArgumentException("Staging directory is configured as null for instance "
                                               + instanceMetadata.host());
        }

        Path stagingDir = Path.of(instanceMetadata.stagingDir());
        Files.createDirectories(stagingDir);
        return stagingDir;
    }
}
