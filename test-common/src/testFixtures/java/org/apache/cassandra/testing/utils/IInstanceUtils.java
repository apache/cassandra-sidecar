/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.testing.utils;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;

/**
 * Utilities for the {@link IInstance}
 */
public class IInstanceUtils
{
    public static int tryGetIntConfig(IInstance instance, String configName, int defaultValue)
    {
        return tryGetIntConfig(instance.config(), configName, defaultValue);
    }

    public static int tryGetIntConfig(IInstanceConfig config, String configName, int defaultValue)
    {
        try
        {
            return config.getInt(configName);
        }
        catch (NullPointerException npe)
        {
            return defaultValue;
        }
    }

    public static List<InetSocketAddress> buildContactList(IInstance instance)
    {
        IInstanceConfig config = instance.config();
        return Collections.singletonList(new InetSocketAddress(config.broadcastAddress().getAddress(),
                                                               tryGetIntConfig(config, "native_transport_port", 9042)));
    }

    public static List<InetSocketAddress> buildContactList(List<IInstanceConfig> configs)
    {
        // Always return the complete list of addresses even if the cluster isn't yet that large
        // this way, we populate the entire local instance list
        return configs.stream()
                      .filter(Objects::nonNull)
                      .map(config -> new InetSocketAddress(config.broadcastAddress().getAddress(),
                                                           tryGetIntConfig(config, "native_transport_port", 9042)))
                      .collect(Collectors.toList());
    }
}
