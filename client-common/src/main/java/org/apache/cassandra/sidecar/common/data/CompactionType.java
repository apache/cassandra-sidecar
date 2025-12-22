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
    CLEANUP,
    SCRUB,
    UPGRADE_SSTABLE,
    VERIFY,
    MAJOR_COMPACTION,
    RELOCATE,
    GARBAGE_COLLECT,
    FLUSH,
    WRITE,
    ANTICOMPCATION,
    VALIDATION,
    INDEX_BUILD,
    VIEW_BUILD,
    COMPACTION,
    TOMBSTONE_COMPACTION,
    STREAM,
    KEY_CACHE_SAVE,
    ROW_CACHE_SAVE,
    COUNTER_CACHE_SAVE,
    INDEX_SUMMARY;

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
            throw new IllegalArgumentException("Unsupported compaction_type: '" + name + "'");
        }
    }
}
