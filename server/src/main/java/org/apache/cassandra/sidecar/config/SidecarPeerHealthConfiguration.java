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
 * Configuration for Sidecar peers health checks
 */
public interface SidecarPeerHealthConfiguration extends PeriodicTaskConfiguration
{
    /**
     * @return the number of maximum retries to be performed during a Sidecar peer health check
     */
    int healthCheckRetries();

    /**
     * @return the delay between Sidecar peer health checks retries
     */
    MillisecondBoundConfiguration healthCheckRetryDelay();
}
