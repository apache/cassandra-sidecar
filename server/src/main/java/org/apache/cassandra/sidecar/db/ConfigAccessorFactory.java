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
import com.google.inject.Inject;
import org.apache.cassandra.sidecar.routes.cdc.Service;

/**
 * Factory for creating config objects based on the service name.
 */
public class ConfigAccessorFactory
{
    private final Map<Service, ConfigAccessor> configAccessors = new HashMap<>();

    @Inject
    public ConfigAccessorFactory(KafkaConfigAccessor kafkaConfigAccessor,
                                 CdcConfigAccessor cdcConfigAccessor)
    {
        configAccessors.put(Service.KAFKA, kafkaConfigAccessor);
        configAccessors.put(Service.CDC, cdcConfigAccessor);
    }

    public ConfigAccessor getConfigAccessor(final Service service)
    {
        if (configAccessors.containsKey(service))
        {
            return configAccessors.get(service);
        }
        throw new RuntimeException("Couldn't find a db accessor for service " + service);
    }
}
