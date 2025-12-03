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

package org.apache.cassandra.sidecar.common.data;

import java.util.Locale;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Supported compaction types based on Cassandra's OperationType enum
 */
public enum CompactionType
{
    COMPACTION,
    VALIDATION,
    KEY_CACHE_SAVE,
    ROW_CACHE_SAVE,
    COUNTER_CACHE_SAVE,
    CLEANUP,
    SCRUB,
    UPGRADE_SSTABLES,
    INDEX_BUILD,
    TOMBSTONE_COMPACTION,
    ANTICOMPACTION,
    VERIFY,
    VIEW_BUILD,
    INDEX_SUMMARY,
    RELOCATE,
    GARBAGE_COLLECT,
    WRITE;

    @Override
    public String toString()
    {
        return name().toLowerCase(Locale.ROOT);
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
            return valueOf(name.toUpperCase(Locale.ROOT));
        }
        catch (IllegalArgumentException unknownEnum)
        {
            throw new IllegalArgumentException("Unsupported compaction_type: '" + name + "'");
        }
    }
}
