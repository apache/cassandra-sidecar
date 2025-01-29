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

package org.apache.cassandra.sidecar.adapters.base.data;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.management.openmbean.CompositeData;

import static org.apache.cassandra.sidecar.adapters.base.data.CompositeDataUtil.extractValue;

/**
 * Representation of session info data
 */
public class SessionInfo
{
    public final String peer;
    public final int sessionIndex;
    public final String connecting;
    /** Immutable collection of receiving summaries */
    public final List<StreamSummary> receivingSummaries;
    /** Immutable collection of sending summaries*/
    public final List<StreamSummary> sendingSummaries;
    /** Current session state */
    public final String state;
    public final List<ProgressInfo> receivingFiles;
    public final List<ProgressInfo> sendingFiles;

    public SessionInfo(CompositeData data)
    {
        this.peer = extractValue(data, "peer");
        this.sessionIndex = extractValue(data, "sessionIndex");
        this.connecting = extractValue(data, "connecting");
        this.receivingSummaries = parseSummaries(extractValue(data, "receivingSummaries"));
        this.sendingSummaries = parseSummaries(extractValue(data, "sendingSummaries"));
        this.state = extractValue(data, "state");
        this.receivingFiles = parseFiles(extractValue(data, "receivingFiles"));
        this.sendingFiles = parseFiles(extractValue(data, "sendingFiles"));
    }

    /**
     * @return total size(in bytes) already received.
     */
    public long totalSizeReceived()
    {
        return totalSizeInProgress(receivingFiles);
    }

    /**
     * @return total size(in bytes) already sent.
     */
    public long totalSizeSent()
    {
        return totalSizeInProgress(sendingFiles);
    }

    /**
     * @return total number of files to receive in the session
     */
    public long totalFilesToReceive()
    {
        return totalFiles(receivingSummaries);
    }

    /**
     * @return total number of files to send in the session
     */
    public long totalFilesToSend()
    {
        return totalFiles(sendingSummaries);
    }

    /**
     * @return total size(in bytes) to receive in the session
     */
    public long totalSizeToReceive()
    {
        return totalSizes(receivingSummaries);
    }

    /**
     * @return total size(in bytes) to send in the session
     */
    public long totalSizeToSend()
    {
        return totalSizes(sendingSummaries);
    }

    /**
     * @return total number of files already received.
     */
    public long totalFilesReceived()
    {
        return totalFilesCompleted(receivingFiles);
    }

    /**
     * @return total number of files already sent.
     */
    public long totalFilesSent()
    {
        return totalFilesCompleted(sendingFiles);
    }

    private long totalSizes(List<StreamSummary> summaries)
    {
        long total = 0;
        for (StreamSummary summary : summaries)
            total += summary.totalSize;
        return total;
    }

    private long totalFilesCompleted(List<ProgressInfo> files)
    {
                return files.stream()
                    .filter(ProgressInfo::isCompleted)
                    .count();
    }

    private long totalSizeInProgress(List<ProgressInfo> streams)
    {
        long total = 0;
        for (ProgressInfo stream : streams)
            total += stream.currentBytes;
        return total;
    }

    private List<StreamSummary> parseSummaries(CompositeData[] summaries)
    {
        return Arrays.stream(summaries).map(StreamSummary::new).collect(Collectors.toList());
    }

    private List<ProgressInfo> parseFiles(CompositeData[] files)
    {
        return Arrays.stream(files).map(ProgressInfo::new).collect(Collectors.toList());
    }

    private long totalFiles(List<StreamSummary> summaries)
    {
        long total = 0;
        for (StreamSummary summary : summaries)
            total += summary.files;
        return total;
    }
}
