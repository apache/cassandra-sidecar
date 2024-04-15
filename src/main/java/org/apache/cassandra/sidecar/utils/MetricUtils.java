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

package org.apache.cassandra.sidecar.utils;

import java.util.Objects;

/**
 * Utilities for Metrics related operations
 */
public class MetricUtils
{
    private static final String DATA = "Data.db";
    private static final String COMPRESSION = "CompressionInfo.db";
    private static final String DIGEST = "Digest.crc32";
    private static final String FILTER = "Filter.db";
    private static final String INDEX = "Index.db";
    private static final String STATISTICS = "Statistics.db";
    private static final String SUMMARY = "Summary.db";

    /**
     * Returns extracted SSTable component. For e.g. for nb-1-big-Data.db component returns Data.db. This is used for
     * marking metrics that are captured specific to SSTable components.
     *
     * @param filename name of SSTable component
     * @return SSTable component, if none found "default" is returned
     */
    public static String parseSSTableComponent(String filename)
    {
        Objects.requireNonNull(filename, "Filename can not be null for SSTable component parsing");
        String trimmedFilename = filename.trim();
        if (trimmedFilename.endsWith(DATA))
        {
            return DATA;
        }
        if (trimmedFilename.endsWith(COMPRESSION))
        {
            return COMPRESSION;
        }
        if (trimmedFilename.endsWith(DIGEST))
        {
            return DIGEST;
        }
        if (trimmedFilename.endsWith(FILTER))
        {
            return FILTER;
        }
        if (trimmedFilename.endsWith(INDEX))
        {
            return INDEX;
        }
        if (trimmedFilename.endsWith(STATISTICS))
        {
            return STATISTICS;
        }
        if (trimmedFilename.endsWith(SUMMARY))
        {
            return SUMMARY;
        }
        return "default";
    }
}
