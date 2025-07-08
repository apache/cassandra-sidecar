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

package org.apache.cassandra.sidecar.exceptions;

/**
 * Contains exception classes specific to Live Migration operations in Cassandra Sidecar.
 * These exceptions handle various error conditions that can occur during Live Migration
 * data copy tasks, including concurrent task conflicts, invalid requests, and task management errors.
 */
public class LiveMigrationExceptions
{

    /**
     * There can be only one Live Migration task running at any time. This exception is thrown when client is trying
     * to create a new Live Migration data copy task while another task is in progress.
     */
    public static class LiveMigrationDataCopyInProgressException extends IllegalArgumentException
    {
        public LiveMigrationDataCopyInProgressException(String message)
        {
            super(message);
        }
    }

    /**
     * Exception thrown when a Live Migration request contains invalid parameters or violates
     *  constraints such as:
     * <ul>
     * <li>Requested max concurrency exceeds system limits</li>
     * <li>Source instance has more data directories than destination</li>
     * <li>Invalid task configuration parameters</li>
     * </ul>
     */
    public static class LiveMigrationInvalidRequestException extends IllegalArgumentException
    {
        public LiveMigrationInvalidRequestException(String message)
        {
            super(message);
        }
    }

    /**
     * Exception thrown when attempting to access a Live Migration task that does not exist.
     */
    public static class LiveMigrationTaskNotFoundException extends IllegalArgumentException
    {
        public LiveMigrationTaskNotFoundException(String message)
        {
            super(message);
        }
    }
}
