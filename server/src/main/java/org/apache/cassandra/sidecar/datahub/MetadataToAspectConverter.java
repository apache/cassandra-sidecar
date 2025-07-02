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

import com.linkedin.data.template.RecordTemplate;
import datahub.event.MetadataChangeProposalWrapper;
import org.jetbrains.annotations.NotNull;

/**
 * Base abstract class that any convertor should implement directly or indirectly.
 * <p>
 * Should only contain logic shared across all metadata entities and aspect types.
 */
abstract class MetadataToAspectConverter<T extends RecordTemplate>
{
    protected static final String DELIMITER = ".";

    @NotNull
    protected final IdentifiersProvider identifiers;

    protected MetadataToAspectConverter(@NotNull IdentifiersProvider identifiers)
    {
        this.identifiers = identifiers;
    }

    /**
     * A helper method that implementing classes should use in order to create a new instance of {@link MetadataChangeProposalWrapper}; the instance will have
     * its {@code type}, {@code urn}, and {@code aspect} fields initialized with the provided values
     *
     * @param type the type to initialize the wrapper with
     * @param urn the URN to initialize the wrapper with
     * @param aspect the aspect to initialize the wrapper with
     * @return a new instance of the wrapper that has been initialized
     */
    @NotNull
    @SuppressWarnings({ "unchecked"})
    protected MetadataChangeProposalWrapper<T> wrap(@NotNull String type,
                                                    @NotNull String urn,
                                                    @NotNull T aspect)
    {
        return MetadataChangeProposalWrapper.builder()
                                            .entityType(type)
                                            .entityUrn(urn)
                                            .upsert()
                                            .aspect(aspect)
                                            .build();
    }
}
