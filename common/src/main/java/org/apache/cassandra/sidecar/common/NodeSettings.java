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

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Holds information about the specific node settings
 */
public class NodeSettings
{
    private final String releaseVersion;
    private final String partitioner;

    /**
     * Constructs a new {@link NodeSettings} object with the Cassandra node's release version and partitioner
     * information.
     *
     * @param releaseVersion the release version of the Cassandra node
     * @param partitioner    the partitioner used by the Cassandra node
     */
    public NodeSettings(@JsonProperty("releaseVersion") String releaseVersion,
                        @JsonProperty("partitioner") String partitioner)
    {
        this.releaseVersion = releaseVersion;
        this.partitioner = partitioner;
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

    /**
     * {@inheritDoc}
     */
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeSettings that = (NodeSettings) o;
        return Objects.equals(releaseVersion, that.releaseVersion)
               && Objects.equals(partitioner, that.partitioner);
    }

    /**
     * {@inheritDoc}
     */
    public int hashCode()
    {
        return Objects.hash(releaseVersion, partitioner);
    }
}
