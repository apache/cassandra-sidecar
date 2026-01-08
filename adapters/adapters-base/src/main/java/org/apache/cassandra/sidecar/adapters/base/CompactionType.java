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
package org.apache.cassandra.sidecar.adapters.base;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Supported compaction types based on Cassandra's OperationType enum
 */
public enum CompactionType
{
    CLEANUP,
    SCRUB,
    UPGRADE_SSTABLES,
    VERIFY,
    RELOCATE,
    GARBAGE_COLLECT,
    ANTICOMPACTION,
    VALIDATION,
    INDEX_BUILD,
    VIEW_BUILD,
    COMPACTION,
    TOMBSTONE_COMPACTION,
    KEY_CACHE_SAVE,
    ROW_CACHE_SAVE,
    COUNTER_CACHE_SAVE,
    INDEX_SUMMARY;

    private static final List<String> SUPPORTED_COMPACTION_TYPES =
            Arrays.stream(org.apache.cassandra.sidecar.adapters.base.CompactionType.values())
                    .map(org.apache.cassandra.sidecar.adapters.base.CompactionType::name)
                    .collect(Collectors.toList());

    @Override
    public String toString()
    {
        return name();
    }

    /**
     * Case-insensitive factory method for Jackson deserialization
     * @return {@link CompactionType} from string
     */
    @JsonCreator
    public static CompactionType fromString(String name)
    {
        if (name == null || name.trim().isEmpty())
        {
            return null;
        }
        try
        {
            return valueOf(name.trim().toUpperCase(Locale.ROOT));
        }
        catch (IllegalArgumentException unknownEnum)
        {
            throw new IllegalArgumentException(
                    String.format("Unsupported compactionType: '%s'. Valid types are: %s",
                            name, SUPPORTED_COMPACTION_TYPES));
        }
    }
}
