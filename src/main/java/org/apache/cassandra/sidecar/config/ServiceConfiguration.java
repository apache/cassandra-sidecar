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

import java.util.Map;

/**
 * Configuration for the Sidecar Service and configuration of the REST endpoints in the service
 */
public interface ServiceConfiguration
{
    String SERVICE_POOL = "service";
    String INTERNAL_POOL = "internal";

    /**
     * @return Sidecar's HTTP REST API listen address
     */
    String host();

    /**
     * @return Sidecar's HTTP REST API port
     */
    int port();

    /**
     * Determines if a connection will timeout and be closed if no data is received nor sent within the timeout.
     * Zero means don't timeout.
     *
     * @return the configured idle timeout value
     */
    int requestIdleTimeoutMillis();

    /**
     * @return the amount of time in millis when a response is considered as timed-out after data has not been written
     */
    long requestTimeoutMillis();

    /**
     * @return the maximum time skew allowed between the server and the client
     */
    int allowableSkewInMinutes();

    /**
     * @return the throttling configuration
     */
    ThrottleConfiguration throttleConfiguration();

    /**
     * @return the configuration for SSTable component uploads on this service
     */
    SSTableUploadConfiguration ssTableUploadConfiguration();

    /**
     * @return the configuration for the SSTable Import functionality
     */
    SSTableImportConfiguration ssTableImportConfiguration();

    /**
     * @return the configured worker pools for the service
     */
    Map<String, ? extends WorkerPoolConfiguration> workerPoolsConfiguration();

    /**
     * @return the configuration for the {@link #SERVICE_POOL}
     */
    default WorkerPoolConfiguration serverWorkerPoolConfiguration()
    {
        return workerPoolsConfiguration().get(SERVICE_POOL);
    }

    /**
     * @return the configuration for the {@link #INTERNAL_POOL}
     */
    default WorkerPoolConfiguration serverInternalWorkerPoolConfiguration()
    {
        return workerPoolsConfiguration().get(INTERNAL_POOL);
    }

    /**
     * @return the system-wide JMX configuration settings
     */
    JmxConfiguration jmxConfiguration();
}
