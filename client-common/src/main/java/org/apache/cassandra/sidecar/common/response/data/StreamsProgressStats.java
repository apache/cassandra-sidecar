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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A class representing stats summarizing the progress of streamed bytes and files on the node
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class StreamsProgressStats
{
    private final long totalFilesToReceive;
    private final long totalFilesReceived;
    private final long totalBytesToReceive;
    private final long totalBytesReceived;
    private final long totalFilesToSend;
    private final long totalFilesSent;
    private final long totalBytesToSend;
    private final long totalBytesSent;

    @JsonCreator
    public StreamsProgressStats(@JsonProperty("totalFilesToReceive") long totalFilesToReceive,
                                @JsonProperty("totalFilesReceived") long totalFilesReceived,
                                @JsonProperty("totalBytesToReceive") long totalBytesToReceive,
                                @JsonProperty("totalBytesReceived") long totalBytesReceived,
                                @JsonProperty("totalFilesToSend") long totalFilesToSend,
                                @JsonProperty("totalFilesSent") long totalFilesSent,
                                @JsonProperty("totalBytesToSend") long totalBytesToSend,
                                @JsonProperty("totalBytesSent") long totalBytesSent)
    {
        this.totalFilesToReceive = totalFilesToReceive;
        this.totalFilesReceived = totalFilesReceived;
        this.totalBytesToReceive = totalBytesToReceive;
        this.totalBytesReceived = totalBytesReceived;
        this.totalFilesToSend = totalFilesToSend;
        this.totalFilesSent = totalFilesSent;
        this.totalBytesToSend = totalBytesToSend;
        this.totalBytesSent = totalBytesSent;
    }

    @JsonProperty("totalFilesToReceive")
    public long totalFilesToReceive()
    {
        return totalFilesToReceive;
    }

    @JsonProperty("totalFilesReceived")
    public long totalFilesReceived()
    {
        return totalFilesReceived;
    }

    @JsonProperty("totalBytesToReceive")
    public long totalBytesToReceive()
    {
        return totalBytesToReceive;
    }

    @JsonProperty("totalBytesReceived")
    public long totalBytesReceived()
    {
        return totalBytesReceived;
    }

    @JsonProperty("totalFilesToSend")
    public long totalFilesToSend()
    {
        return totalFilesToSend;
    }

    @JsonProperty("totalFilesSent")
    public long totalFilesSent()
    {
        return totalFilesSent;
    }

    @JsonProperty("totalBytesToSend")
    public long totalBytesToSend()
    {
        return totalBytesToSend;
    }

    @JsonProperty("totalBytesSent")
    public long totalBytesSent()
    {
        return totalBytesSent;
    }
}
