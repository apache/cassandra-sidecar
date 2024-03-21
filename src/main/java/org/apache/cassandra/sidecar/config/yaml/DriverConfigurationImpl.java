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

package org.apache.cassandra.sidecar.config.yaml;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.config.DriverConfiguration;

/**
 * The driver configuration to use when connecting to Cassandra
 */
public class DriverConfigurationImpl implements DriverConfiguration
{
    private final List<InetSocketAddress> contactPoints = new ArrayList<>();
    private String localDc;
    private int numConnections;

    @Override
    @JsonProperty("contact_points")
    public List<InetSocketAddress> contactPoints()
    {
        return contactPoints;
    }

    @Override
    @JsonProperty("num_connections")
    public int numConnections()
    {
        return numConnections;
    }

    @Override
    @JsonProperty("local_dc")
    public String localDc()
    {
        return localDc;
    }
}
