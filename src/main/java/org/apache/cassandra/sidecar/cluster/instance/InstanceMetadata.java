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

package org.apache.cassandra.sidecar.cluster.instance;

import java.util.List;

import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.jetbrains.annotations.Nullable;


/**
 * Metadata of an instance
 */
public interface InstanceMetadata
{
    /**
     * @return an identifier for the Cassandra instance
     */
    int id();

    /**
     * @return the host address of the Cassandra instance
     */
    String host();

    /**
     * @return the port number of the Cassandra instance
     */
    int port();

    /**
     * @return a list of data directories of cassandra instance
     */
    List<String> dataDirs();

    /**
     * @return a staging directory of the cassandra instance
     */
    String stagingDir();

    /**
     * @return a {@link CassandraAdapterDelegate} specific for the instance
     */
    @Nullable CassandraAdapterDelegate delegate();
}
