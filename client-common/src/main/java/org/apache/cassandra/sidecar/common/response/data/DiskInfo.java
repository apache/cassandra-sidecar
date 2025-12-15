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

package org.apache.cassandra.sidecar.common.response.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents information about a single disk. It contains space metrics (total, free, usable)
 * and identifying information (name, mount point, type).
 */
public class DiskInfo
{

    private final long totalSpace;
    private final long freeSpace;
    private final long usableSpace;
    private final String name;
    private final String mount;
    private final String type;

    @JsonCreator
    public DiskInfo(@JsonProperty("totalSpace") long totalSpace,
                    @JsonProperty("freeSpace") long freeSpace,
                    @JsonProperty("usableSpace") long usableSpace,
                    @JsonProperty("name") String name,
                    @JsonProperty("mount") String mount,
                    @JsonProperty("type") String type)
    {
        this.totalSpace = totalSpace;
        this.freeSpace = freeSpace;
        this.usableSpace = usableSpace;
        this.name = name;
        this.mount = mount;
        this.type = type;
    }

    @JsonProperty("totalSpace")
    public long totalSpace()
    {
        return totalSpace;
    }

    @JsonProperty("freeSpace")
    public long freeSpace()
    {
        return freeSpace;
    }

    @JsonProperty("usableSpace")
    public long usableSpace()
    {
        return usableSpace;
    }

    @JsonProperty("name")
    public String name()
    {
        return name;
    }

    @JsonProperty("mount")
    public String mount()
    {
        return mount;
    }

    @JsonProperty("type")
    public String type()
    {
        return type;
    }
}
