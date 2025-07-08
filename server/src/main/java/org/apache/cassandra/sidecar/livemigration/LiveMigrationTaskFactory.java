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

package org.apache.cassandra.sidecar.livemigration;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.request.LiveMigrationDataCopyRequest;


/**
 * Factory interface for creating {@link LiveMigrationTask} instances. This factory is necessary because 
 * LiveMigrationTask instances require both injected dependencies and runtime parameters from the request,
 * making direct dependency injection impossible. The factory pattern allows proper dependency injection
 * while enabling dynamic task creation with request-specific parameters.
 */
public interface LiveMigrationTaskFactory
{
    /**
     * Creates a live migration task. Newly created task will not start downloading files on its own.
     * It has to be started explicitly using {@link LiveMigrationTask#start()}.
     *
     * @param request          Live migration request
     * @param source           Source from which data should be downloaded
     * @param port             Source sidecar port
     * @param instanceMetadata Instance metadata for which data copy should be initiated.
     * @return Live migration task
     */
    LiveMigrationTask create(LiveMigrationDataCopyRequest request,
                             String source,
                             int port,
                             InstanceMetadata instanceMetadata);
}
