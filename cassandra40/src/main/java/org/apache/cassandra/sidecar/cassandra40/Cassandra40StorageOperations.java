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

package org.apache.cassandra.sidecar.cassandra40;

import java.util.Map;

import org.apache.cassandra.sidecar.common.JmxClient;
import org.apache.cassandra.sidecar.common.StorageOperations;

/**
 * An implementation of the {@link StorageOperations} that interfaces with Cassandra 4.0
 */
public class Cassandra40StorageOperations implements StorageOperations
{
    private static final String STORAGE_SERVICE_OBJ_NAME = "org.apache.cassandra.db:type=StorageService";
    private final JmxClient jmxClient;

    /**
     * Creates a new instance with the provided {@link JmxClient}
     *
     * @param jmxClient the JMX client used to communicate with the Cassandra instance
     */
    public Cassandra40StorageOperations(JmxClient jmxClient)
    {
        this.jmxClient = jmxClient;
    }

    /**
     * {@inheritDoc}
     */
    public void takeSnapshot(String tag, Map<String, String> options, String... entities)
    {
        jmxClient.call(StorageOperations.class, STORAGE_SERVICE_OBJ_NAME,
                       ssProxy -> ssProxy.takeSnapshot(tag, options, entities));
    }
}
