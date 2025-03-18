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

package org.apache.cassandra.sidecar.coordination;

import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

/**
 * An implementation of {@link ElectorateMembership} where the current Sidecar will
 * be determined to be part of the electorate iff one of the Cassandra instances it
 * manages owns token {@code 0} for the {@code sidecar_internal} keyspace.
 */
public class SidecarInternalTokenZeroElectorateMembership extends AbstractTokenZeroOfKeyspaceElectorateMembership
{
    private final SchemaKeyspaceConfiguration configuration;

    public SidecarInternalTokenZeroElectorateMembership(InstanceMetadataFetcher instanceMetadataFetcher,
                                                        SidecarConfiguration sidecarConfiguration)
    {
        super(instanceMetadataFetcher);
        configuration = sidecarConfiguration.serviceConfiguration().schemaKeyspaceConfiguration();
    }

    @Override
    protected String keyspaceToDetermineElectorateMembership()
    {
        return configuration.keyspace();
    }
}
