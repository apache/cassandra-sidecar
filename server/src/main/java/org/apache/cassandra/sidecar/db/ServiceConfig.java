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
package org.apache.cassandra.sidecar.db;

import java.util.HashMap;
import java.util.Map;
import com.google.common.collect.ImmutableMap;
import com.datastax.driver.core.Row;
import org.jetbrains.annotations.Nullable;

/**
 * In-memory representation of service configs stored in "configs" table in C*
 */
public class ServiceConfig
{
    private final Map<String, String> serviceConfig;

    public ServiceConfig()
    {
        this.serviceConfig = new HashMap<>();
    }

    public ServiceConfig(Map<String, String> serviceConfig)
    {
        this.serviceConfig = serviceConfig;
    }

    public static ServiceConfig from(@Nullable Row row)
    {
        if (row == null || row.isNull(0))
        {
            return new ServiceConfig();
        }
        Map<String, String> configMap = row.getMap("config", String.class, String.class);
        return new ServiceConfig(configMap);
    }

    public Map<String, String> getConfigs()
    {
        return ImmutableMap.copyOf(serviceConfig);
    }
}
