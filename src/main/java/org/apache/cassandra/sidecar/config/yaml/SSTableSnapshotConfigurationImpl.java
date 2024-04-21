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

package org.apache.cassandra.sidecar.config.yaml;

import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.SSTableSnapshotConfiguration;

/**
 * Configuration for the SSTable Snapshot functionality
 */
public class SSTableSnapshotConfigurationImpl implements SSTableSnapshotConfiguration
{
    protected static final CacheConfiguration DEFAULT_SNAPSHOT_LIST_CACHE_CONFIGURATION =
    new CacheConfigurationImpl(TimeUnit.HOURS.toMillis(2), 10_000);

    @JsonProperty(value = "snapshot_list_cache")
    protected CacheConfiguration snapshotListCacheConfiguration;

    public SSTableSnapshotConfigurationImpl()
    {
        this(DEFAULT_SNAPSHOT_LIST_CACHE_CONFIGURATION);
    }

    public SSTableSnapshotConfigurationImpl(CacheConfiguration snapshotListCacheConfiguration)
    {
        this.snapshotListCacheConfiguration = snapshotListCacheConfiguration;
    }

    /**
     * @return the configuration for the cache used for SSTable snapshot list of files
     */
    @Override
    @JsonProperty(value = "snapshot_list_cache")
    public CacheConfiguration snapshotListCacheConfiguration()
    {
        return snapshotListCacheConfiguration;
    }
}
