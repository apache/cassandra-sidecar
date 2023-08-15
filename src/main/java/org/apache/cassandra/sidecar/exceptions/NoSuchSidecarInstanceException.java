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

import java.util.NoSuchElementException;

/**
 * Thrown when the Sidecar instance is not found in the metadata
 */
public class NoSuchSidecarInstanceException extends NoSuchElementException
{
    /**
     * Constructs a {@link NoSuchSidecarInstanceException}, saving a reference
     * to the error message string {@code errorMessage} for later retrieval by the
     * {@code getMessage} method.
     *
     * @param errorMessage the detail message.
     */
    public NoSuchSidecarInstanceException(String errorMessage)
    {
        super(errorMessage);
    }

    /**
     * Constructs a {@link NoSuchSidecarInstanceException} with the specified detail
     * message and cause.
     *
     * @param message the detail message (which is saved for later retrieval
     *                by the {@link #getMessage()} method).
     * @param cause   the cause (which is saved for later retrieval by the
     *                {@link #getCause()} method).  (A {@code null} value is
     *                permitted, and indicates that the cause is nonexistent or
     *                unknown.)
     */
    public NoSuchSidecarInstanceException(String message, Throwable cause)
    {
        super(message);
        initCause(cause);
    }
}
