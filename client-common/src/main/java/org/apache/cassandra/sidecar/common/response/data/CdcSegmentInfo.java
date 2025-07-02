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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * Class representing segment information.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CdcSegmentInfo
{
    public final String name;
    public final long size;
    public final long idx;
    public final boolean completed;
    public final long lastModifiedTimestamp;

    public CdcSegmentInfo(@JsonProperty("name") String name,
                          @JsonProperty("size") long size,
                          @JsonProperty("idx") long idx,
                          @JsonProperty("completed") boolean completed,
                          @JsonProperty("lastModifiedTimestamp") long lastModifiedTimestamp)
    {
        this.name = name;
        this.size = size;
        this.idx = idx;
        this.completed = completed;
        this.lastModifiedTimestamp = lastModifiedTimestamp;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        CdcSegmentInfo that = (CdcSegmentInfo) o;
        return size == that.size && idx == that.idx && completed == that.completed && lastModifiedTimestamp == that.lastModifiedTimestamp
                && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, size, idx, completed, lastModifiedTimestamp);
    }
}
