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

package org.apache.cassandra.sidecar.metrics.instance;

/**
 * {@link InstanceMetrics} tracks metrics related to a Cassandra instance that Sidecar maintains.
 */
public interface InstanceMetrics
{
    String INSTANCE_PREFIX = "sidecar.cas_instance";

    /**
     * @return resource metrics tracked for cassandra instance
     */
    InstanceResourceMetrics resource();

    /**
     * @return metrics that are tracked during streaming of SSTable components from a cassandra instance
     */
    StreamSSTableMetrics streamSSTable();

    /**
     * @return metrics that are tracked during upload of SSTable components to a cassandra instance
     */
    UploadSSTableMetrics uploadSSTable();
}
