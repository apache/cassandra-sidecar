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

package org.apache.cassandra.sidecar.datahub;

import com.google.common.collect.ImmutableMap;

import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataplatforminstance.DataPlatformInstanceProperties;
import datahub.event.MetadataChangeProposalWrapper;
import org.jetbrains.annotations.NotNull;

/**
 * Converter class for preparing the Data Platform Instance Properties aspect for a given Cassandra cluster
 */
public class ClusterToDataPlatformInstancePropertiesConverter extends ClusterToAspectConverter<DataPlatformInstanceProperties>
{
    protected static final String ENVIRONMENT = "environment";
    protected static final String APPLICATION = "application";
    protected static final String CLUSTER     = "cluster";

    public ClusterToDataPlatformInstancePropertiesConverter(@NotNull IdentifiersProvider identifiers)
    {
        super(identifiers);
    }

    @Override
    @NotNull
    public MetadataChangeProposalWrapper<DataPlatformInstanceProperties> convert(@NotNull Metadata cluster)
    {
        String type = IdentifiersProvider.DATA_PLATFORM_INSTANCE;

        String urn = identifiers.urnDataPlatformInstance();

        DataPlatformInstanceProperties aspect = new DataPlatformInstanceProperties()
                .setName(identifiers.identifier().toString())
                .setDescription(null, SetMode.REMOVE_IF_NULL)  // Cluster-level comments are not supported by Cassandra
                .setCustomProperties(new StringMap(ImmutableMap.of(
                        ENVIRONMENT, identifiers.environment(),
                        APPLICATION, identifiers.application(),
                        CLUSTER,     identifiers.cluster())));

        return wrap(type, urn, aspect);
    }
}
