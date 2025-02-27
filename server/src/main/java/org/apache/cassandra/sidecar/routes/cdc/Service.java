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
package org.apache.cassandra.sidecar.routes.cdc;

import java.util.HashMap;
import java.util.Map;

/**
 * Enum representing various services inside config table in sidecar internal keyspace.
 */
public enum Service
{
    KAFKA("kafka"),
    CDC("cdc");

    public final String serviceName;
    private static final Map<String, Service> LOOKUP = new HashMap<>();

    static
    {
        for (Service service : Service.values())
        {
            LOOKUP.put(service.serviceName, service);
        }
    }

    Service(final String serviceName)
    {
        this.serviceName = serviceName;
    }

    public static Service withName(String serviceName)
    {
        if (LOOKUP.containsKey(serviceName))
        {
            return LOOKUP.get(serviceName);
        }
        throw new RuntimeException("Invalid service name " + serviceName);
    }
}
