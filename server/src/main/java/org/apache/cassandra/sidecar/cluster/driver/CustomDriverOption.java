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

package org.apache.cassandra.sidecar.cluster.driver;

import com.datastax.oss.driver.api.core.config.DriverOption;
import org.jspecify.annotations.NonNull;

/**
 * Custom driver options required by {@link SidecarLoadBalancingPolicy}.
 */
public enum CustomDriverOption implements DriverOption
{
    /**
     * Number of non-localhost Cassandra connections to be maintained by the Sidecar.
     */
    NUM_CONNECTIONS("basic.load-balancing-policy.num-connections"),

    /**
     * Comma-separated list of local Cassandra instances in the format of {@code host:port}.
     */
    LOCAL_INSTANCES("basic.load-balancing-policy.local-instances");

    private final String path;

    CustomDriverOption(String path)
    {
        this.path = path;
    }

    @Override
    @NonNull
    public String getPath()
    {
        return path;
    }
}
