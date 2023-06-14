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

import java.net.UnknownHostException;

/**
 * An interface that pulls methods from Cassandra endpoint snitch info
 */
public interface EndpointSnitchJmxOperations
{
    String ENDPOINT_SNITCH_INFO_OBJ_NAME = "org.apache.cassandra.db:type=EndpointSnitchInfo";

    /**
     * Provides the Rack name depending on the respective snitch used, given the host name/ip
     *
     * @param host the ip or hostname
     * @throws UnknownHostException when the host is not resolved
     */
    String getRack(String host) throws UnknownHostException;

    /**
     * Provides the Datacenter name depending on the respective snitch used, given the hostname/ip
     *
     * @param host the ip or hostname
     * @throws UnknownHostException when the host is not resolved
     */
    String getDatacenter(String host) throws UnknownHostException;
}
