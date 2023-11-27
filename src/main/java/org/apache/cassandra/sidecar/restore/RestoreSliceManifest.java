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

package org.apache.cassandra.sidecar.restore;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.utils.ImmutableMap;

/**
 * Schema for the manifest file in slice
 * It is essentially the mapping from SSTable identifier (String) to the ManifestEntry.
 */
public class RestoreSliceManifest extends HashMap<String, RestoreSliceManifest.ManifestEntry>
{
    public static final String MANIFEST_FILE_NAME = "manifest.json";

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger LOGGER = LoggerFactory.getLogger(RestoreSliceManifest.class);

    public static RestoreSliceManifest read(File file) throws RestoreJobFatalException
    {
        if (!file.exists())
        {
            throw new RestoreJobFatalException("Manifest file does not exist. file: " + file);
        }

        try
        {
            return MAPPER.readValue(file, RestoreSliceManifest.class);
        }
        catch (IOException e)
        {
            LOGGER.error("Failed to read restore slice manifest. file={}", file, e);
            throw new RestoreJobFatalException("Unable to read manifest", e);
        }
    }

    /**
     * Merge all checksums of all the SSTable components included in the manifest
     * @return map of file name to checksum; it never returns null
     */
    public @NotNull Map<String, String> mergeAllChecksums()
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        values().forEach(entry -> entry.componentsChecksum()
                                       .forEach(builder::put));
        return builder.build();
    }

    /**
     * Manifest entry that contains the checksum of each component in the SSTable and the token range
     */
    public static class ManifestEntry
    {
        private final Map<String, String> componentsChecksum;
        private final BigInteger startToken;
        private final BigInteger endToken;

        @JsonCreator
        public ManifestEntry(@JsonProperty("components_checksum") Map<String, String> componentsChecksum,
                             @JsonProperty("start_token") BigInteger startToken,
                             @JsonProperty("end_token") BigInteger endToken)
        {
            this.componentsChecksum = componentsChecksum;
            this.startToken = startToken;
            this.endToken = endToken;
        }

        @JsonProperty("components_checksum")
        public Map<String, String> componentsChecksum()
        {
            return componentsChecksum;
        }

        @JsonProperty("start_token")
        public BigInteger startToken()
        {
            return startToken;
        }

        @JsonProperty("end_token")
        public BigInteger endToken()
        {
            return endToken;
        }
    }
}
