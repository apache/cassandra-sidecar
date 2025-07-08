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

import org.apache.cassandra.sidecar.common.request.LiveMigrationDataCopyRequest;
import org.apache.cassandra.sidecar.common.response.LiveMigrationTaskResponse;

/**
 * Represents a live migration data copy task. During live migration, the source Cassandra instance
 * may continue running while the destination copies files, meaning new SSTables can be created and
 * existing ones may disappear due to compaction. To handle these changes, the task performs multiple
 * iterations to synchronize data until the success threshold criteria in
 * {@link LiveMigrationDataCopyRequest#successThreshold} is met, up to a maximum number of iterations
 * specified by {@link LiveMigrationDataCopyRequest#maxIterations}.
 */
public interface LiveMigrationTask
{
    /**
     * ID of live migration task.
     *
     * @return task id.
     */
    String id();

    /**
     * Starts live migration.
     */
    void start();

    /**
     * Gets the task details including status which can be passed to clients as response.
     * State can change as the task makes progress.
     *
     * @return current live migration task's response
     */
    LiveMigrationTaskResponse getResponse();

    /**
     * Cancels the current live migration task if not finished already.
     */
    void cancel();

    /**
     * Tells whether current live migration has completed or not.
     *
     * @return true if completed otherwise false.
     */
    boolean isCompleted();
}
