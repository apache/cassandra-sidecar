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

package org.apache.cassandra.sidecar.utils;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.sidecar.common.ICassandraFactory;
import org.jetbrains.annotations.VisibleForTesting;


/**
 * Manages multiple Cassandra versions
 */
public class CassandraVersionProvider
{
    final ArrayList<ICassandraFactory> versions;

    public CassandraVersionProvider(ArrayList<ICassandraFactory> versions)
    {
        this.versions = versions;
    }

    @VisibleForTesting
    public List<ICassandraFactory> allVersions()
    {
        return this.versions;
    }

    /**
     * For the provided CassandraVersion, return a new ICassandraFactory instance
     * that meets the minimum version requirements
     * That factory can be used to create an ICassandraAdapter
     *
     * @param requestedVersion the requested Cassandra version
     * @return the factory for the requested Cassandra version
     */
    public ICassandraFactory cassandra(SimpleCassandraVersion requestedVersion)
    {
        ICassandraFactory result = versions.get(0);

        for (ICassandraFactory factory : versions)
        {
            SimpleCassandraVersion currentMinVersion = SimpleCassandraVersion.create(result);
            SimpleCassandraVersion nextVersion = SimpleCassandraVersion.create(factory);

            // skip if we can rule this out early
            if (nextVersion.isGreaterThan(requestedVersion)) continue;

            if (requestedVersion.isGreaterThan(currentMinVersion))
            {
                result = factory;
            }
        }
        return result;
    }

    /**
     * Convenience method for getCassandra, converts the String version to a typed one
     *
     * @param requestedVersion the version string to parse
     * @return the Cassandra Factory implementation for the input {@code requestedVersion}
     * @throws IllegalArgumentException if the provided string does not
     *                                  represent a version
     */
    public ICassandraFactory cassandra(String requestedVersion)
    {
        SimpleCassandraVersion version = SimpleCassandraVersion.create(requestedVersion);
        return cassandra(version);
    }

    /**
     * Builder for VersionProvider
     */
    @NotThreadSafe
    public static class Builder
    {
        ArrayList<ICassandraFactory> versions;

        public Builder()
        {
            versions = new ArrayList<>();
        }

        public CassandraVersionProvider build()
        {
            if (versions.isEmpty())
            {
                throw new IllegalStateException("At least one ICassandraFactory is required");
            }
            return new CassandraVersionProvider(versions);
        }

        public Builder add(ICassandraFactory version)
        {
            versions.add(version);
            return this;
        }
    }
}
