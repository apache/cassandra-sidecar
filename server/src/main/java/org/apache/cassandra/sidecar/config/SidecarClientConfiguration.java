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

import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;

/**
 * Configuration for sidecar client
 */
public interface SidecarClientConfiguration
{
    /**
     * @return {@code true} if SSL should be used for Sidecar client connections
     */
    boolean useSsl();

    /**
     * @return the client request timeout value for the connection to be established
     */
    MillisecondBoundConfiguration requestTimeout();

    /**
     * @return the client idle timeout before the connection is considered as stale
     */
    MillisecondBoundConfiguration requestIdleTimeout();

    // Pooling options

    /**
     * @return the maximum size of the pool for client connections
     */
    int connectionPoolMaxSize();

    /**
     * @return the connection pool cleaner period, a non-positive value disables expiration checks and connections
     * will remain in the pool until they are closed.
     */
    MillisecondBoundConfiguration connectionPoolCleanerPeriod();

    /**
     * Return the configured number of event-loop the pool use.
     *
     * <ul>
     *   <li>when the size is {@code 0}, the client pool will use the current event-loop</li>
     *   <li>otherwise the client will create and use its own event loop</li>
     * </ul>
     *
     * @return the configured number of event-loop the pool use
     */
    int connectionPoolEventLoopSize();

    /**
     * @return the maximum requests allowed in the wait queue, any requests beyond the max size will result in
     * a ConnectionPoolTooBusyException.  If the value is set to a negative number then the queue will be unbounded.
     */
    int connectionPoolMaxWaitQueueSize();

    /**
     * @return the maximum number of retries for a failed call.
     */
    int maxRetries();

    /**
     * @return the delay between retries.
     */
    MillisecondBoundConfiguration retryDelay();

    /**
     * @return the max delay between retries.
     */
    MillisecondBoundConfiguration maxRetryDelay();
}
