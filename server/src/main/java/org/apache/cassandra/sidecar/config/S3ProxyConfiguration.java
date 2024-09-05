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

package org.apache.cassandra.sidecar.config;

import java.net.URI;

/**
 * Configuration for S3 proxy.
 */
public interface S3ProxyConfiguration
{
    /**
     * @return proxy URI
     */
    URI proxy();

    /**
     * @return proxy username
     */
    String username();

    /**
     * @return proxy password
     */
    String password();

    /**
     * Optional/nullable; only used for testing. Specify the override so the s3 client can connect to local S3
     * testing server.
     *
     * @return the S3 client override used for testing only
     */
    URI endpointOverride();

    default boolean isPresent()
    {
        return proxy() != null;
    }
}
