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

import java.net.URISyntaxException;

import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.Urn;
import datahub.event.MetadataChangeProposalWrapper;
import org.jetbrains.annotations.NotNull;

/**
 * Converter class for preparing the Data Platform Instance aspect for a given Cassandra keyspace
 */
public class KeyspaceToDataPlatformInstanceConverter extends KeyspaceToAspectConverter<DataPlatformInstance>
{
    public KeyspaceToDataPlatformInstanceConverter(@NotNull IdentifiersProvider identifiers)
    {
        super(identifiers);
    }

    @Override
    @NotNull
    public MetadataChangeProposalWrapper<DataPlatformInstance> convert(@NotNull KeyspaceMetadata keyspace) throws URISyntaxException
    {
        String urn = identifiers.urnContainer(keyspace);

        DataPlatformInstance aspect = new DataPlatformInstance()
                .setPlatform(new Urn(identifiers.urnDataPlatform()))
                .setInstance(new Urn(identifiers.urnDataPlatformInstance()));

        return wrap(urn, aspect);
    }
}
