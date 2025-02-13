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
package org.apache.cassandra.sidecar.common.request.data;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A class representing a response for the GetServicesConfig api "/api/v1/services" which contains
 * configs for all the services in the "configs" table.
 */
public class GetServicesConfigPayload
{
    public final List<Service> services;

    @JsonCreator
    public GetServicesConfigPayload(@JsonProperty("services") List<Service> services)
    {
        this.services = services;
    }

    @JsonProperty("services")
    public List<Service> services()
    {
        return services;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetServicesConfigPayload that = (GetServicesConfigPayload) o;
        return Objects.equals(services, that.services);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(services);
    }

    /**
     * POJO class that represent config for a given service in "configs" table in sidecar
     * internal keyspace
     */
    public static class Service
    {
        public final String service;
        public final Map<String, String> config;

        @JsonCreator
        public Service(@JsonProperty("service") String service,
                       @JsonProperty("config") Map<String, String> config)
        {
            this.service = service;
            this.config = config;
        }

        @JsonProperty("service")
        public String service()
        {
            return service;
        }

        @JsonProperty("config")
        public Map<String, String> config()
        {
            return config;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Service service1 = (Service) o;
            return Objects.equals(service, service1.service) && Objects.equals(config, service1.config);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(service, config);
        }
    }
}
