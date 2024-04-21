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

package org.apache.cassandra.sidecar.snapshots;

import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;

@ExtendWith(VertxExtension.class)
class SnapshotPathBuilderTest extends AbstractSnapshotPathBuilderTest
{
    @Override
    public SnapshotPathBuilder initialize(Vertx vertx, ServiceConfiguration serviceConfiguration,
                                          InstancesConfig instancesConfig,
                                          CassandraInputValidator validator, ExecutorPools executorPools)
    {
        return new SnapshotPathBuilder(vertx, instancesConfig, validator, executorPools);
    }
}
