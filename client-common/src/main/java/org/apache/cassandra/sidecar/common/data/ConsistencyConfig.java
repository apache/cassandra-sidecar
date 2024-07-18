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

import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.sidecar.common.utils.StringUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Consistency config to group the related values, i.e. consistency level and local datacenter name.
 * Note that both fields are nullable
 */
public class ConsistencyConfig
{
    @Nullable
    public final ConsistencyLevel consistencyLevel;
    @Nullable
    public final String localDatacenter;

    /**
     * Create {@link ConsistencyConfig} from the string literals
     *
     * @param consistencyLevelString nullable string to resolve to {@link ConsistencyLevel}
     * @param localDatacenter nullable string; required to be not empty when the resolved consistency level is localDcOnly
     * @return {@link ConsistencyConfig} instance
     */
    public static ConsistencyConfig parseString(@Nullable String consistencyLevelString, @Nullable String localDatacenter)
    {
        return new ConsistencyConfig(ConsistencyLevel.fromString(consistencyLevelString), localDatacenter);
    }

    private ConsistencyConfig(@Nullable ConsistencyLevel consistencyLevel, @Nullable String localDatacenter)
    {
        Preconditions.checkArgument(consistencyLevel == null || !consistencyLevel.isLocalDcOnly || StringUtils.isNotEmpty(localDatacenter),
                                    "localDatacenter cannot be empty for consistency level: " + consistencyLevel);

        this.consistencyLevel = consistencyLevel;
        this.localDatacenter = localDatacenter;
    }
}
