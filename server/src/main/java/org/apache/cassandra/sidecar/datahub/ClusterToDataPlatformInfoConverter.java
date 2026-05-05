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

import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.dataplatform.PlatformType;
import datahub.event.MetadataChangeProposalWrapper;
import org.jetbrains.annotations.NotNull;

/**
 * Converter class for preparing the Data Platform Info aspect for a given Cassandra cluster
 */
public class ClusterToDataPlatformInfoConverter extends ClusterToAspectConverter<DataPlatformInfo>
{
    public ClusterToDataPlatformInfoConverter(@NotNull IdentifiersProvider identifiers)
    {
        super(identifiers);
    }

    @Override
    @NotNull
    public MetadataChangeProposalWrapper<DataPlatformInfo> convert(@NotNull Metadata cluster)
    {
        String type = IdentifiersProvider.DATA_PLATFORM;

        String urn = identifiers.urnDataPlatform();

        DataPlatformInfo aspect = new DataPlatformInfo()
                .setType(PlatformType.RELATIONAL_DB)
                .setName(identifiers.platform())
                .setDisplayName(identifiers.organization())
                .setDatasetNameDelimiter(DELIMITER);

        return wrap(type, urn, aspect);
    }
}
