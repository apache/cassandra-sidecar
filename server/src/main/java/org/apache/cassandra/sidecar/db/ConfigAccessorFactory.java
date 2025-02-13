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

import com.google.inject.Inject;
import org.apache.cassandra.sidecar.routes.cdc.ValidServices;

/**
 * Factory for creating config objects based on the service name.
 */
public class ConfigAccessorFactory
{
    private final KafkaConfigAccessor kafkaConfigAccessor;
    private final CdcConfigAccessor cdcConfigAccessor;

    @Inject
    public ConfigAccessorFactory(KafkaConfigAccessor kafkaConfigAccessor,
                                 CdcConfigAccessor cdcConfigAccessor)
    {
        this.kafkaConfigAccessor = kafkaConfigAccessor;
        this.cdcConfigAccessor = cdcConfigAccessor;
    }

    public ConfigAccessor getConfigAccessor(final String service)
    {
        if (service.equals(ValidServices.KAFKA.serviceName))
        {
            return kafkaConfigAccessor;
        }

        if (service.equals(ValidServices.CDC.serviceName))
        {
            return cdcConfigAccessor;
        }

        throw new RuntimeException("Couldn't find a db accessor for service " + service);
    }
}
