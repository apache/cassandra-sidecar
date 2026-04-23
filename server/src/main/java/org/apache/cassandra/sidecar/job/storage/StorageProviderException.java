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

package org.apache.cassandra.sidecar.job.storage;

/**
 * A runtime exception that {@link StorageProvider} implementations should wrap their
 * backend-specific errors in. This gives callers a single exception type to catch regardless
 * of the underlying backend.
 * <p>
 * Implementations should catch backend-specific exceptions (e.g., {@code DriverException}
 * for Cassandra, {@code SQLException} for JDBC) and wrap them in {@code StorageProviderException}
 * with the original exception as the cause.
 */
public class StorageProviderException extends RuntimeException
{
    /**
     * Constructs a new StorageProviderException with the specified detail message.
     *
     * @param message the detail message
     */
    public StorageProviderException(String message)
    {
        super(message);
    }

    /**
     * Constructs a new StorageProviderException with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause   the underlying cause
     */
    public StorageProviderException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
