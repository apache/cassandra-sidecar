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

package org.apache.cassandra.sidecar.cluster;

import java.util.List;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;

/**
 * Maintains metadata of instances maintained by Sidecar.
 */
public interface InstancesConfig
{
    /**
     * @return metadata of instances owned by the sidecar
     */
    List<InstanceMetadata> instances();

    /**
     * Lookup instance metadata by id.
     *
     * @param id instance's id
     * @return instance meta information
     * @throws IllegalArgumentException when the instance with {@code id} does not exist
     */
    InstanceMetadata instanceFromId(int id) throws IllegalArgumentException;

    /**
     * Lookup instance metadata by host name.
     *
     * @param host host address of instance
     * @return instance meta information
     * @throws IllegalArgumentException when the instance for {@code host} does not exist
     */
    InstanceMetadata instanceFromHost(String host) throws IllegalArgumentException;
}
