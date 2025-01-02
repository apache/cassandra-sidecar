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

package org.apache.cassandra.sidecar.common.response;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.response.data.CdcSegmentInfo;

/**
 * A class representing a response for a list CDC Segment request
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ListCdcSegmentsResponse
{
    private final String host;
    private final int port;
    private final List<CdcSegmentInfo> segmentsInfo;

    @JsonCreator
    public ListCdcSegmentsResponse(@JsonProperty("host") String host,
                                   @JsonProperty("port") int port,
                                   @JsonProperty("segmentsInfo") List<CdcSegmentInfo> segmentsInfo)
    {
        this.host = host;
        this.port = port;
        this.segmentsInfo = segmentsInfo == null ? Collections.emptyList() : Collections.unmodifiableList(segmentsInfo);
    }

    @JsonProperty("host")
    public String host()
    {
        return host;
    }

    @JsonProperty("port")
    public int port()
    {
        return port;
    }

    @JsonProperty("segmentsInfo")
    public List<CdcSegmentInfo> segmentsInfo()
    {
        return segmentsInfo;
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
        ListCdcSegmentsResponse that = (ListCdcSegmentsResponse) o;
        return port == that.port && Objects.equals(host, that.host) && Objects.equals(segmentsInfo, that.segmentsInfo);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(host, port, segmentsInfo);
    }
}
