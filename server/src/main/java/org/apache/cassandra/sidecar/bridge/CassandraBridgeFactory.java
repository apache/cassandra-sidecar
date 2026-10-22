/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.bridge;

import java.util.Optional;

import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CassandraVersionFeatures;
import org.apache.cassandra.bridge.CdcBridge;
import org.apache.cassandra.bridge.CdcBridgeFactory;
import org.apache.cassandra.cdc.avro.CqlToAvroSchemaConverter;
import org.jetbrains.annotations.NotNull;

/**
 * Single point for Sidecar to obtain version-specific Cassandra bridges. Sidecar code should use this factory
 * instead of calling {@link CdcBridgeFactory} directly.
 * <p>
 * All instances are provided by {@link CdcBridgeFactory}, which loads them in one classloader per Cassandra version.
 * As a result, the returned {@link CassandraBridge} is loaded together with the CDC classes: the {@link CdcBridge}
 * and the Avro converter ({@link CqlToAvroSchemaConverter}). That classloader contains the Cassandra, bridge and
 * types jars of a plain {@link CassandraBridge}, plus the Avro converter jar.
 * <p>
 * Using a single classloader is required for CDC: types created by one classloader (e.g. UDTs built while parsing
 * the schema) cannot be cast to the same classes in another, which fails CDC deserialization with a
 * {@link ClassCastException}.
 */
public class CassandraBridgeFactory
{
    /**
     * Resolves the Cassandra version of a release version. Every CDC lookup by release version goes through this
     * method, so overriding it changes how all of them resolve a release version.
     *
     * @param releaseVersion Cassandra release version, e.g. {@code "5.0.4"}
     * @return the Cassandra version, or empty if no Cassandra version matches the release version
     */
    @NotNull
    public Optional<CassandraVersion> cassandraVersion(@NotNull String releaseVersion)
    {
        return CassandraVersion.fromVersion(releaseVersion);
    }

    /**
     * @param version Cassandra release version, e.g. {@code "5.0.4"}
     * @return the Cassandra bridge for the version
     * @throws IllegalArgumentException if no Cassandra version matches the release version
     */
    @NotNull
    public CassandraBridge get(@NotNull String version)
    {
        CassandraVersion cassandraVersion = cassandraVersion(version)
                                            .orElseThrow(() -> new IllegalArgumentException("Cassandra version " + version + " is not supported"));
        return get(cassandraVersion);
    }

    /**
     * @param features Cassandra version features
     * @return the Cassandra bridge for the version
     */
    @NotNull
    public CassandraBridge get(@NotNull CassandraVersionFeatures features)
    {
        return CdcBridgeFactory.get(features);
    }

    /**
     * @param version Cassandra version
     * @return the Cassandra bridge for the version
     */
    @NotNull
    public CassandraBridge get(@NotNull CassandraVersion version)
    {
        return CdcBridgeFactory.get(version);
    }

    /**
     * @param bridge Cassandra bridge obtained from this factory
     * @return the CDC bridge for the same Cassandra version, loaded in the same classloader as {@code bridge}
     */
    @NotNull
    public CdcBridge getCdcBridge(@NotNull CassandraBridge bridge)
    {
        return CdcBridgeFactory.getCdcBridge(bridge);
    }

    /**
     * @param bridge Cassandra bridge obtained from this factory
     * @return the CQL to Avro schema converter for the same Cassandra version, loaded in the same classloader
     * as {@code bridge}
     */
    @NotNull
    public CqlToAvroSchemaConverter getCqlToAvroSchemaConverter(@NotNull CassandraBridge bridge)
    {
        return CdcBridgeFactory.getCqlToAvroSchemaConverter(bridge);
    }
}
