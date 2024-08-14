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

/**
 * Configuration needed for S3 client used by Sidecar for purposes like restoring, etc.
 */
public interface S3ClientConfiguration
{
    String threadNamePrefix();

    /**
     * @return the maximum concurrency/parallelism of the thread pool used by S3 client
     */
    int concurrency();

    /**
     * @return the timeout of idle threads
     */
    long threadKeepAliveSeconds();

    /**
     * Returns range bytes size to produce <a href="https://www.rfc-editor.org/rfc/rfc9110.html#name-range">Range header</a> for range-get object.
     * The size should not be too large (long request) or too small (too many request). 5 to 10 MiB would be ideal to start with.
     * @return range bytes size.
     */
    int rangeGetObjectBytesSize();

    /**
     * API call timeout in milliseconds for S3 API calls.
     * @return duration of timeout in milliseconds
     */
    long apiCallTimeoutMillis();

    /**
     * Route traffic through a proxy in the environment that requires doing so, when a proxy is specified
     *
     * @return the proxy configuration, if specified
     */
    S3ProxyConfiguration proxyConfig();
}
