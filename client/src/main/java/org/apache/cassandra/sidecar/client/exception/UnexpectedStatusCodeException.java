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

package org.apache.cassandra.sidecar.client.exception;

/**
 * Represents an exception raised when an unexpected status code is encountered
 */
public class UnexpectedStatusCodeException extends RuntimeException
{
    /**
     * Constructs a new {@link UnexpectedStatusCodeException} with the given {@code statusCode} and {@code body}
     *
     * @param statusCode the response status code
     * @param body       the body of the response
     */
    public UnexpectedStatusCodeException(int statusCode, String body)
    {
        this(getMessage(statusCode, body), null);
    }

    /**
     * Constructs a new {@link UnexpectedStatusCodeException} with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause   the cause
     */
    public UnexpectedStatusCodeException(String message, Throwable cause)
    {
        super(message, cause);
    }

    private static String getMessage(int statusCode, String body)
    {
        String message = "Unexpected HTTP status code " + statusCode;
        return body == null ? message : message + "\n\n" + body;
    }
}
