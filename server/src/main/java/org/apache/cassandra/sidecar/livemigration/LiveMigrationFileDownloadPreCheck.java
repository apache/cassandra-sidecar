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
import org.apache.cassandra.sidecar.common.request.LiveMigrationDataCopyRequest;

/**
 * A pluggable pre-check hook that runs before each file download iteration in the live migration
 * data copy process. Since the data copy involves deleting local files and downloading files from
 * the source, implementations can perform safety validations (e.g., verifying cluster state via gossip,
 * checking instance readiness) to prevent data corruption or unsafe operations.
 *
 * <p>The pre-check is invoked at the beginning of every download iteration (not just the first one),
 * allowing implementations to continuously validate that conditions remain safe throughout the
 * multi-iteration copy process.</p>
 *
 * <p>A {@link #DEFAULT} no-op implementation is provided for cases where no pre-check is needed.
 * Custom implementations can be bound via Guice to override the default behavior.</p>
 *
 * <p>Example use cases:</p>
 * <ul>
 *   <li>Gossip-based validation: verify the source node is present and the destination node
 *       is absent from cluster gossip, preventing data copy to a node that has already joined</li>
 *   <li>Instance state checks: verify the local Cassandra process is not running</li>
 *   <li>Disk space validation: ensure sufficient space before downloading</li>
 * </ul>
 *
 * @see LiveMigrationFileDownloader#downloadFiles()
 */
public interface LiveMigrationFileDownloadPreCheck
{
    /**
     * Default no-op implementation that always succeeds. Used when no pre-check validation is required.
     */
    LiveMigrationFileDownloadPreCheck DEFAULT = context -> Future.succeededFuture();

    /**
     * Performs a safety check before a file download iteration begins.
     *
     * <p>Implementations should return a succeeded future if the check passes (it is safe to proceed),
     * or a failed future with a descriptive exception if the check fails (download should be aborted).</p>
     *
     * @param context provides access to the source host, destination instance metadata,
     *                sidecar port, and the data copy request parameters needed for validation
     * @return a succeeded {@link Future} if the pre-check passes, a failed {@link Future} otherwise
     */
    Future<Void> doCheck(PreCheckContext context);

    /**
     * Encapsulates the contextual information available to a {@link LiveMigrationFileDownloadPreCheck}
     * implementation. This context is populated by the {@link LiveMigrationFileDownloader} before each
     * download iteration.
     *
     * <p>Provides access to:</p>
     * <ul>
     *   <li>{@link #source()} - hostname of the source instance being copied from</li>
     *   <li>{@link #destinationInstanceMetadata()} - metadata of the local/destination instance,
     *       including host, data directories, and the Cassandra adapter delegate</li>
     *   <li>{@link #sidecarPort()} - port on which Sidecar is running, useful for making
     *       Sidecar API calls to other cluster instances</li>
     *   <li>{@link #request()} - the original data copy request containing task parameters</li>
     * </ul>
     */
    interface PreCheckContext
    {
        /**
         * @return the hostname of the source instance from which files are being copied
         */
        String source();

        /**
         * @return the metadata for the destination (local) instance where files will be written
         */
        InstanceMetadata destinationInstanceMetadata();

        /**
         * @return the Sidecar service port, useful for contacting other Sidecar instances in the cluster
         */
        int sidecarPort();

        /**
         * @return the original data copy request containing task parameters such as max iterations,
         *         success threshold, and max concurrency
         */
        LiveMigrationDataCopyRequest request();
    }
}
