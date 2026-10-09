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
     * Thrown when attempting to create a new live migration task while another task is already in progress
     * for the same instance. Only one live migration task (e.g., data copy, file digest verification) can be
     * active per instance at a time to prevent resource conflicts and ensure data integrity.
     */
    public static class LiveMigrationTaskInProgressException extends IllegalStateException
    {
        public LiveMigrationTaskInProgressException(String message)
        {
            super(message);
        }
    }

    /**
     * Exception thrown when a Live Migration request contains invalid parameters or violates
     * constraints such as:
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

        public LiveMigrationInvalidRequestException(String message, Throwable cause)
        {
            super(message, cause);
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

    /**
     * Thrown when a live-migration file URL is well-formed but matches no configured directory prefix
     * on this instance, i.e. it addresses no resource here. Extends {@link IllegalArgumentException} so
     * that destination-side callers - which map file URLs obtained from the source's file-list API onto
     * local paths - keep treating an unmatched prefix (a source/destination directory-configuration
     * mismatch) as a bad argument, while the source-side request handler can catch this specific type to
     * distinguish "no such resource" (HTTP 404) from a malformed URL (HTTP 400).
     */
    public static class UnknownMigrationPrefixException extends IllegalArgumentException
    {
        public UnknownMigrationPrefixException(String message)
        {
            super(message);
        }
    }

    /**
     * Exception thrown when file verification fails during live migration.
     */
    public static class FileVerificationFailureException extends Exception
    {
        public FileVerificationFailureException(String message)
        {
            super(message);
        }
    }

    /**
     * Exception thrown when file digest verification fails during live migration.
     * This indicates that the digest of a file at the destination does not match
     * the digest of the same file at the source, suggesting data corruption or
     * incomplete file transfer.
     */
    public static class DigestMismatchException extends Exception
    {
        private final String path;
        private final String fileUrl;

        public DigestMismatchException(String path, String fileUrl, Throwable cause)
        {
            super("Digest mismatch for file: " + fileUrl + " (local path: " + path + ")", cause);
            this.path = path;
            this.fileUrl = fileUrl;
        }

        public String path()
        {
            return path;
        }

        public String fileUrl()
        {
            return fileUrl;
        }
    }
}
