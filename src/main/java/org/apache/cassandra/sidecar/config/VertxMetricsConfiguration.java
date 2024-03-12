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

import java.util.List;

/**
 * Holds configuration needed for enabling metrics capture in Vert.x framework, for analyzing from Sidecar.
 */
public interface VertxMetricsConfiguration
{
    /**
     * @return boolean indicating whether metrics captured is enabled for Vert.x
     */
    boolean enabled();

    /**
     * @return registry name to be used for registering Vert.x metrics
     */
    String registryName();

    /**
     * @return boolean indicating whether Vert.x metrics will be exposed via JMX
     */
    boolean exposeViaJMX();

    /**
     * @return JMX domain to be used when metrics are exposed via JMX
     */
    String jmxDomainName();

    /**
     * @return List of regexes used for identifying what server routes will be monitored
     */
    List<String> monitoredServerRouteRegexes();
}
