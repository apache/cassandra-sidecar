/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.common.data;

import java.util.Locale;

import org.jetbrains.annotations.Nullable;

/**
 * Consistency level for read/write operations
 * Note: not supporting all consistency level in Cassandra on purpose
 */
public enum ConsistencyLevel
{
    ONE,
    TWO,
    LOCAL_ONE(true),
    LOCAL_QUORUM(true),
    EACH_QUORUM,
    QUORUM,
    ALL;

    public final boolean isLocalDcOnly;

    ConsistencyLevel()
    {
        this(false);
    }

    ConsistencyLevel(boolean isLocalDcOnly)
    {
        this.isLocalDcOnly = isLocalDcOnly;
    }

    /**
     * Try to get the enum value based on the input name.
     * @param name string literal of the enum name
     * @return enum, or null if the input is null
     */
    public static @Nullable ConsistencyLevel fromString(@Nullable String name)
    {
        return name == null ? null : valueOf(name.toUpperCase(Locale.ROOT));
    }
}
