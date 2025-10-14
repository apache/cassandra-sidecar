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

/**
 * Represents a live migration task. This interface provides lifecycle management
 * for asynchronous live migration operations.
 *
 * @param <T> the type of response returned by this task
 */
public interface LiveMigrationTask<T>
{
    /**
     * ID of live migration task.
     *
     * @return task id.
     */
    String id();

    /**
     * Type of task
     */
    String type();

    /**
     * Starts the live migration task.
     */
    void start();

    /**
     * Gets the task details including status which can be passed to clients as response.
     * State can change as the task makes progress.
     *
     * @return current live migration task's response
     */
    T getResponse();

    /**
     * Cancels the current live migration task if not finished already.
     *
     * <p><b>Note:</b> Cancellation is best-effort; ongoing operations may not stop immediately.
     */
    void cancel();

    /**
     * Tells whether current live migration task has completed or not.
     *
     * @return true if completed otherwise false.
     */
    boolean isCompleted();
}
