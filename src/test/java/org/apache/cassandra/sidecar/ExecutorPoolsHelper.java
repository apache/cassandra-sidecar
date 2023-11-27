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

package org.apache.cassandra.sidecar;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.config.WorkerPoolConfiguration;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.WorkerPoolConfigurationImpl;

import static org.apache.cassandra.sidecar.config.ServiceConfiguration.INTERNAL_POOL;
import static org.apache.cassandra.sidecar.config.ServiceConfiguration.SERVICE_POOL;

/**
 * Test helper for managing worker pool threads.
 */
public class ExecutorPoolsHelper
{
    public static ExecutorPools createdSharedTestPool(Vertx vertx)
    {
        ServiceConfiguration serviceConfiguration
        = ServiceConfigurationImpl.builder()
                                  .workerPoolsConfiguration(buildTestWorkerPoolConfiguration())
                                  .build();
        return new ExecutorPools(vertx, serviceConfiguration);
    }

    public static Map<String, WorkerPoolConfiguration> buildTestWorkerPoolConfiguration()
    {
        WorkerPoolConfiguration workerPoolConf = new WorkerPoolConfigurationImpl("test-pool",
                                                                                 20,
                                                                                 TimeUnit.SECONDS.toMillis(30));
        return Collections.unmodifiableMap(new HashMap<String, WorkerPoolConfiguration>()
        {{
            put(SERVICE_POOL, workerPoolConf);
            put(INTERNAL_POOL, workerPoolConf);
        }});
    }
}
