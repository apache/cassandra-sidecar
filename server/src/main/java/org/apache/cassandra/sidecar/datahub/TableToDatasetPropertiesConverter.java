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

import java.time.Instant;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.linkedin.common.TimeStamp;
import com.linkedin.data.template.SetMode;
import com.linkedin.dataset.DatasetProperties;
import datahub.event.MetadataChangeProposalWrapper;
import org.jetbrains.annotations.NotNull;

/**
 * Converter class for preparing the Dataset Properties aspect for a given Cassandra table
 */
public class TableToDatasetPropertiesConverter extends TableToAspectConverter<DatasetProperties>
{
    private static final CqlIdentifier COMMENT = CqlIdentifier.fromCql("comment");

    public TableToDatasetPropertiesConverter(@NotNull IdentifiersProvider identifiers)
    {
        super(identifiers);
    }

    @Override
    @NotNull
    public MetadataChangeProposalWrapper<DatasetProperties> convert(@NotNull KeyspaceMetadata keyspace,
                                                                    @NotNull TableMetadata table)
    {
        String urn = identifiers.urnDataset(table);

        DatasetProperties aspect = new DatasetProperties()
                .setName(table.getName().asInternal())
                .setQualifiedName(keyspace.getName().asInternal() + DELIMITER + table.getName());

        String comment = (String) table.getOptions().get(COMMENT);
        if (comment != null)
        {
            aspect = aspect
                .setDescription(comment);
        }

        aspect = aspect  // It is desirable to obtain creation and modification timestamps, but the necessary permissions may be lacking
                .setCreated(null, SetMode.REMOVE_IF_NULL)
                .setLastModified(null, SetMode.REMOVE_IF_NULL);

        return wrap(urn, aspect);
    }

    @NotNull
    @SuppressWarnings("unused")
    protected static TimeStamp convertTime(@NotNull Instant javaTime)
    {
        return new TimeStamp()
                .setTime(javaTime.toEpochMilli());
    }
}
