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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.response.LiveMigrationStatus;

/**
 * Implementation of LiveMigrationStatusTracker that manages migration status using files.
 * Tracks completion state by creating/reading JSON files in the staging directory.
 */
@Singleton
public class LiveMigrationStatusTrackerImpl implements LiveMigrationStatusTracker
{

    private static final Logger LOGGER = LoggerFactory.getLogger(LiveMigrationStatusTrackerImpl.class);
    private static final String STATUS_FILE_NAME = "live_migration_status.json";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized LiveMigrationStatus setMigrationCompleted(InstanceMetadata instanceMetadata,
                                                                  long endTime) throws IOException
    {
        Path stagingDir = getStagingDir(instanceMetadata);

        Path statusFilePath = stagingDir.resolve(STATUS_FILE_NAME);

        LiveMigrationStatus status = new LiveMigrationStatus(LiveMigrationStatus.MigrationState.COMPLETED, endTime);
        String content = objectMapper.writeValueAsString(status);

        // Files should be created only once. If attempted to create file one more time,
        // then something is wrong.
        Files.write(statusFilePath, content.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE_NEW);

        LOGGER.info("Live migration file for instance {} successfully created at {}.",
                    instanceMetadata.host(), statusFilePath.toAbsolutePath());

        return status;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized LiveMigrationStatus getMigrationStatus(InstanceMetadata instanceMetadata) throws IOException
    {
        Path stagingDir = getStagingDir(instanceMetadata);
        Path statusFile = stagingDir.resolve(STATUS_FILE_NAME);
        if (!Files.exists(statusFile))
        {
            return null;
        }

        String content = Files.readString(statusFile, StandardCharsets.UTF_8);
        return objectMapper.readValue(content, LiveMigrationStatus.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized boolean hasMigrationCompleted(InstanceMetadata instanceMetadata) throws IOException
    {
        LiveMigrationStatus status = getMigrationStatus(instanceMetadata);
        return status != null && status.state() == LiveMigrationStatus.MigrationState.COMPLETED;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void unsetMigrationCompleted(InstanceMetadata instanceMetadata) throws IOException
    {
        if (!hasMigrationCompleted(instanceMetadata))
        {
            throw new IllegalArgumentException("Live migration status is not set as completed for instance " +
                                               instanceMetadata.host());
        }

        Path stagingDir = getStagingDir(instanceMetadata);
        Path statusFilePath = stagingDir.resolve(STATUS_FILE_NAME);
        Files.deleteIfExists(statusFilePath);
        LOGGER.info("Live migration file for instance {} successfully deleted at {}.",
                    instanceMetadata.host(), statusFilePath.toAbsolutePath());
    }

    private Path getStagingDir(InstanceMetadata instanceMetadata) throws IOException
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
