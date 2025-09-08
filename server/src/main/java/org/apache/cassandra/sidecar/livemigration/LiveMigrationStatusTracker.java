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

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.response.LiveMigrationStatus;

/**
 * Interface for tracking live migration status. Provides methods to manage migration completion states.
 */
public interface LiveMigrationStatusTracker
{

    /**
     * Sets the live migration status as completed for the given instance.
     *
     * @param instanceMetadata Metadata of instance for which migration completion status needs to be recorded
     * @param endTime          timestamp when the migration was completed
     * @return the LiveMigrationStatus object that was created and stored
     * @throws IOException              when cannot record migration completion
     * @throws IllegalArgumentException when staging directory is not configured
     */
    LiveMigrationStatus setMigrationCompleted(InstanceMetadata instanceMetadata, long endTime) throws IOException;

    /**
     * Retrieves the live migration status for the given instance.
     *
     * @param instanceMetadata metadata of the instance for which status is requested
     * @return LiveMigrationStatus object containing the current status, or null if no status is set
     * @throws IOException if unable to read the migration status
     */
    LiveMigrationStatus getMigrationStatus(InstanceMetadata instanceMetadata) throws IOException;

    /**
     * Checks whether the migration has been completed for the given instance by reading and
     * validating the stored migration status.
     *
     * @param instanceMetadata metadata object of instance for which migration status needs to be checked
     * @return true if migration has completed, otherwise false.
     * @throws IOException if unable to read or parse the migration status
     */
    boolean hasMigrationCompleted(InstanceMetadata instanceMetadata) throws IOException;

    /**
     * Unsets the migration completion status for a specific instance.
     * <p>
     * IMPORTANT: The caller must ensure that this method is called ONLY AFTER
     * the instance entry is removed from the live migration map (i.e., after
     * completing the live migration process).
     * <p>
     * Unsetting the migration status is necessary because:
     * 1. If the migration status remains after successful migration, it may prevent
     * future migrations of the same destination instance.
     * 2. Removing the migration status allows the instance to be migrated again in the future.
     *
     * @param instanceMetadata metadata of the instance for which the migration status needs to be cleared
     * @throws IOException if clearing the migration status fails
     */
    void unsetMigrationCompleted(InstanceMetadata instanceMetadata) throws IOException;
}
