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
import com.linkedin.data.template.RecordTemplate;
import datahub.event.MetadataChangeProposalWrapper;
import org.jetbrains.annotations.NotNull;

/**
 * Base abstract class for convertors that handle Cassandra tables (DataHub datasets)
 *
 * @param <T> type of the aspect produced by this converter
 */
public abstract class TableToAspectConverter<T extends RecordTemplate> extends MetadataToAspectConverter<T>
{
    protected TableToAspectConverter(@NotNull IdentifiersProvider identifiers)
    {
        super(identifiers);
    }

    /**
     * A helper method used in order to create a new instance of {@link MetadataChangeProposalWrapper} for a table;
     * the instance will have its {@code urn} and {@code aspect} fields initialized with the provided values
     *
     * @param urn the URN to initialize the wrapper with
     * @param aspect the aspect to initialize the wrapper with
     * @return a new instance of the wrapper that has been initialized
     */
    @NotNull
    protected MetadataChangeProposalWrapper<T> wrap(@NotNull String urn,
                                                    @NotNull T aspect)
    {
        String type = IdentifiersProvider.DATASET;

        return wrap(type, urn, aspect);
    }

    @NotNull
    public abstract MetadataChangeProposalWrapper<T> convert(@NotNull KeyspaceMetadata keyspace,
                                                             @NotNull TableMetadata table) throws Exception;
}
