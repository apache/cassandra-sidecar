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

package org.apache.cassandra.sidecar.common;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds information about the specific node settings
 */
public class NodeSettings
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeSettings.class);
    private static final String SIDECAR_VERSION = getSidecarVersion();

    private final String partitioner;
    private final String releaseVersion;
    private final String sidecarVersion;

    private static String getSidecarVersion()
    {
        try (InputStream version = NodeSettings.class.getResourceAsStream("/sidecar.version"))
        {
            return IOUtils.toString(version, StandardCharsets.UTF_8);
        }
        catch (Exception exception)
        {
            LOGGER.error("Failed to retrieve Sidecar version", exception);
        }
        return "unknown";
    }

    /**
     * Constructs a new {@link NodeSettings} object with the Cassandra node's partitioner
     * and release version information, uses Sidecar version from the currently loaded binary
     *
     * @param partitioner    the partitioner used by the Cassandra node
     * @param releaseVersion the release version of the Cassandra node
     */
    public NodeSettings(String partitioner, String releaseVersion)
    {
        this(partitioner, releaseVersion, SIDECAR_VERSION);
    }

    /**
     * Constructs a new {@link NodeSettings} object with the Cassandra node's partitioner,
     * release version, and Sidecar version information
     *
     * @param partitioner    the partitioner used by the Cassandra node
     * @param releaseVersion the release version of the Cassandra node
     * @param sidecarVersion the version of the Sidecar on the Cassandra node
     */
    public NodeSettings(@JsonProperty("partitioner")    String partitioner,
                        @JsonProperty("releaseVersion") String releaseVersion,
                        @JsonProperty("sidecarVersion") String sidecarVersion)
    {
        this.partitioner = partitioner;
        this.releaseVersion = releaseVersion;
        this.sidecarVersion = sidecarVersion;
    }

    @JsonProperty("partitioner")
    public String partitioner()
    {
        return partitioner;
    }

    @JsonProperty("releaseVersion")
    public String releaseVersion()
    {
        return releaseVersion;
    }

    @JsonProperty("sidecarVersion")
    public String sidecarVersion()
    {
        return sidecarVersion;
    }

    /**
     * {@inheritDoc}
     */
    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }
        if (other == null || this.getClass() != other.getClass())
        {
            return false;
        }
        NodeSettings that = (NodeSettings) other;
        return Objects.equals(this.partitioner,    that.partitioner)
            && Objects.equals(this.releaseVersion, that.releaseVersion)
            && Objects.equals(this.sidecarVersion, that.sidecarVersion);
    }

    /**
     * {@inheritDoc}
     */
    public int hashCode()
    {
        return Objects.hash(partitioner, releaseVersion, sidecarVersion);
    }
}
