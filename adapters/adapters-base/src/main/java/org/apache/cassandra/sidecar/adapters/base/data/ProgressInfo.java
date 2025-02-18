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

import javax.management.openmbean.CompositeData;

import static org.apache.cassandra.sidecar.adapters.base.data.CompositeDataUtil.extractValue;

/**
 * Representation of the stream progress info
 */
public class ProgressInfo
{
    public final String peer;
    public final int sessionIndex;
    public final String fileName;
    public final String direction;
    public final long currentBytes;
    public final long totalBytes;

    public ProgressInfo(CompositeData data)
    {
        this.peer = extractValue(data, "peer");
        this.sessionIndex = extractValue(data, "sessionIndex");
        this.fileName = extractValue(data, "fileName");
        this.direction = extractValue(data, "direction");
        this.currentBytes = extractValue(data, "currentBytes");
        this.totalBytes = extractValue(data, "totalBytes");
    }

    /**
     * @return true if transfer is completed
     */
    public boolean isCompleted()
    {
        return currentBytes >= totalBytes;
    }
}
