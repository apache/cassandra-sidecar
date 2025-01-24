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

package org.apache.cassandra.sidecar.adapters.base;

import java.util.Set;
import javax.management.openmbean.CompositeData;

/**
 * An interface that pulls methods from the Cassandra Stream manager Proxy
 */
public interface StreamManagerJmxOperations
{

    String STREAM_MANAGER_OBJ_NAME = "org.apache.cassandra.net:type=StreamManager";

    /**
     * Returns the current snapshot of the progress of all ongoing streams.
     * @return the current state of streams as a set of JMX {@link CompositeData} instances.
     */
    Set<CompositeData> getCurrentStreams();
}
