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
import java.nio.file.FileAlreadyExistsException;
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
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.response.LiveMigrationStatus;
import org.apache.cassandra.sidecar.common.response.LiveMigrationStatus.MigrationState;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;

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
    private final ExecutorPools executorPools;
    private final ObjectMapper objectMapper;

    // Per-instance locks to ensure atomicity of file operations
    private final ConcurrentHashMap<String, Object> instanceLocks = new ConcurrentHashMap<>();

    @Inject
    public LiveMigrationStatusTrackerImpl(ExecutorPools executorPools)
    {
        this.executorPools = executorPools;
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
        if (null == instanceMetadata)
        {
            return Future.failedFuture(new IllegalArgumentException("instanceMetadata cannot be null"));
        }
        if (null == status)
        {
           return Future.failedFuture(new IllegalArgumentException("Live migration status cannot be null"));
        }

        return executorPools.internal().executeBlocking(() -> {
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
                    return null;
                }
                catch (JsonProcessingException e)
                {
                    LOGGER.error("Failed to serialize LiveMigrationStatus to JSON string. Status={}", status, e);
                    throw e;
                }
                catch (FileAlreadyExistsException e)
                {
                    LOGGER.error("Live migration status file already exists for instance {}. " +
                                 "Status file should be created only once.",
                                 instanceMetadata.host(), e);
                    throw e;
                }
                catch (IOException e)
                {
                    LOGGER.error("Failed to save LiveMigrationStatus. Status={}", status, e);
                    throw e;
                }
            }
        }, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<LiveMigrationStatus> getMigrationStatus(InstanceMetadata instanceMetadata)
    {
        if (null == instanceMetadata)
        {
            return Future.failedFuture(new IllegalArgumentException("instanceMetadata cannot be null"));
        }

        return executorPools.internal().executeBlocking(() -> {
            synchronized (getLockForInstance(instanceMetadata.host()))
            {
                try
                {
                    Path stagingDir = ensureStagingDir(instanceMetadata);
                    Path statusFile = stagingDir.resolve(STATUS_FILE_NAME);

                    if (!Files.exists(statusFile))
                    {
                        return NOT_COMPLETED_STATUS;
                    }

                    String content = Files.readString(statusFile, StandardCharsets.UTF_8);
                    return objectMapper.readValue(content, LiveMigrationStatus.class);
                }
                catch (IOException e)
                {
                    LOGGER.error("Failed to read LiveMigrationStatus for instance {}", instanceMetadata.host(), e);
                    throw e;
                }
            }
        }, false);
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
        if (null == instanceMetadata)
        {
            return Future.failedFuture(new IllegalArgumentException("instanceMetadata cannot be null"));
        }

        return executorPools.internal().executeBlocking(() -> {
            synchronized (getLockForInstance(instanceMetadata.host()))
            {
                try
                {
                    Path stagingDir = ensureStagingDir(instanceMetadata);
                    Path statusFile = stagingDir.resolve(STATUS_FILE_NAME);

                    if (!Files.exists(statusFile))
                    {
                        throw new IllegalStateException(
                        "Live Migration status file does not exist for instance " + instanceMetadata.host());
                    }

                    // Read and verify status
                    String content = Files.readString(statusFile, StandardCharsets.UTF_8);
                    LiveMigrationStatus status = objectMapper.readValue(content, LiveMigrationStatus.class);

                    if (status.state() != MigrationState.COMPLETED)
                    {
                        throw new IllegalStateException(
                        "Live Migration status is not set as completed for instance " + instanceMetadata.host());
                    }

                    // Delete the file
                    Files.delete(statusFile);
                    LOGGER.info("Live migration status file deleted for instance {}", instanceMetadata.host());
                    return null;
                }
                catch (IOException e)
                {
                    LOGGER.error("Failed to clear LiveMigrationStatus for instance {}. " +
                                 "Please check and manually delete the file if needed.", instanceMetadata.host(), e);
                    throw e;
                }
            }
        }, false);
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
