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

import io.vertx.core.Future;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.response.LiveMigrationStatus;

/**
 * Interface for tracking live migration status. Provides methods to manage migration completion states.
 */
public interface LiveMigrationStatusTracker
{

    /**
     * Sets the live migration status for the given instance.
     *
     * @param instanceMetadata Metadata of instance for which migration status needs to be recorded
     * @param status           LiveMigrationStatus object containing the status and timestamp of completion
     * @return a Future that completes when the status has been successfully written
     */
    Future<Void> setMigrationStatus(InstanceMetadata instanceMetadata, LiveMigrationStatus status);

    /**
     * Retrieves the live migration status for the given instance.
     *
     * @param instanceMetadata metadata of the instance for which status is requested
     * @return a Future containing the LiveMigrationStatus object with the current status,
     * or NOT_COMPLETED_STATUS if no status is set
     */
    Future<LiveMigrationStatus> getMigrationStatus(InstanceMetadata instanceMetadata);

    /**
     * Checks whether the migration has been completed for the given instance by reading and
     * validating the stored migration status.
     *
     * @param instanceMetadata metadata object of instance for which migration status needs to be checked
     * @return a Future containing true if migration has completed, otherwise false
     */
    Future<Boolean> hasMigrationCompleted(InstanceMetadata instanceMetadata);

    /**
     * Clears the migration status for a specific instance.
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
     * @return a Future that completes when the migration status has been successfully cleared
     */
    Future<Void> clearMigrationStatus(InstanceMetadata instanceMetadata);
}
