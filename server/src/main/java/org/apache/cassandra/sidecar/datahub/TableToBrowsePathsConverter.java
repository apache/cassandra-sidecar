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

import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.linkedin.common.BrowsePaths;
import com.linkedin.data.template.StringArray;
import datahub.event.MetadataChangeProposalWrapper;
import org.jetbrains.annotations.NotNull;

/**
 * Converter class for preparing the Browse Paths aspect
 */
public class TableToBrowsePathsConverter extends TableToAspectConverter<BrowsePaths>
{
    protected static final String PROD = "prod";  // DataHub requires this to be {@code prod} regardless

    public TableToBrowsePathsConverter(@NotNull IdentifiersProvider identifiers)
    {
        super(identifiers);
    }

    @Override
    @NotNull
    public MetadataChangeProposalWrapper<BrowsePaths> convert(@NotNull KeyspaceMetadata keyspace,
                                                              @NotNull TableMetadata table)
    {
        String urn = identifiers.urnDataset(table);

        String path = String.format("/%s/%s/%s/%s/%s/%s/",
                PROD,
                identifiers.platform(),
                identifiers.environment(),
                identifiers.application(),
                identifiers.cluster(),
                table.getKeyspace().asInternal());

        BrowsePaths aspect = new BrowsePaths()
                .setPaths(new StringArray(path));

        return wrap(urn, aspect);
    }
}
