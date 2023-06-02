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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds information about the specific node settings
 */
public class NodeSettings
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeSettings.class);
    private static final String VERSION = "version";
    private static final Map<String, String> SIDECAR = Collections.singletonMap(VERSION, getSidecarVersion());

    private final String releaseVersion;
    private final String partitioner;
    private final Map<String, String> sidecar;

    private static String getSidecarVersion()
    {
        final String resource = "/sidecar.version";
        try (InputStream input = NodeSettings.class.getResourceAsStream(resource);
             ByteArrayOutputStream output = new ByteArrayOutputStream())
        {
            byte[] buffer = new byte[32];
            int length;
            while ((length = input.read(buffer)) >= 0)
            {
                output.write(buffer, 0, length);
            }
            return output.toString(StandardCharsets.UTF_8.name());
        }
        catch (Exception exception)
        {
            LOGGER.error("Failed to retrieve Sidecar version", exception);
        }
        return "unknown";
    }

    /**
     * Constructs a new {@link NodeSettings} object with the Cassandra node's release version
     * and partitioner information, uses Sidecar version from the currently loaded binary
     *
     * @param releaseVersion the release version of the Cassandra node
     * @param partitioner    the partitioner used by the Cassandra node
     */
    public NodeSettings(String releaseVersion, String partitioner)
    {
        this(releaseVersion, partitioner, SIDECAR);
    }

    /**
     * Constructs a new {@link NodeSettings} object with the Cassandra node's release version,
     * partitioner, and Sidecar version information
     *
     * @param releaseVersion the release version of the Cassandra node
     * @param partitioner    the partitioner used by the Cassandra node
     * @param sidecar        the settings of the Sidecar on the Cassandra node, including its version
     */
    public NodeSettings(@JsonProperty("releaseVersion") String releaseVersion,
                        @JsonProperty("partitioner")    String partitioner,
                        @JsonProperty("sidecar")        Map<String, String> sidecar)
    {
        this.releaseVersion = releaseVersion;
        this.partitioner    = partitioner;
        this.sidecar        = sidecar;
    }

    @JsonProperty("releaseVersion")
    public String releaseVersion()
    {
        return releaseVersion;
    }

    @JsonProperty("partitioner")
    public String partitioner()
    {
        return partitioner;
    }

    @JsonProperty("sidecar")
    public Map<String, String> sidecar()
    {
        return sidecar;
    }

    public String sidecarVersion()
    {
        return sidecar.get(VERSION);
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
        return Objects.equals(this.releaseVersion, that.releaseVersion)
            && Objects.equals(this.partitioner,    that.partitioner)
            && Objects.equals(this.sidecar,        that.sidecar);
    }

    /**
     * {@inheritDoc}
     */
    public int hashCode()
    {
        return Objects.hash(releaseVersion, partitioner, sidecar);
    }
}
