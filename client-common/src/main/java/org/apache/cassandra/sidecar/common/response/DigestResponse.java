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

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.request.data.Digest;
import org.apache.cassandra.sidecar.common.request.data.MD5Digest;
import org.apache.cassandra.sidecar.common.request.data.XXHash32Digest;

/**
 * Response object containing a cryptographic digest value for file verification purposes.
 */
public class DigestResponse
{
    @JsonProperty("digest")
    public final String digest;
    @JsonProperty("digestAlgorithm")
    public final String digestAlgorithm;

    @JsonCreator
    public DigestResponse(@JsonProperty("digest") String digest,
                          @JsonProperty("digestAlgorithm") String digestAlgorithm)
    {
        this.digest = Objects.requireNonNull(digest, "digest is required");
        this.digestAlgorithm = Objects.requireNonNull(digestAlgorithm, "digestAlgorithm is required");
    }

    @Override
    public String toString()
    {
        return "DigestResponse{" +
               "digest='" + digest + '\'' +
               ", digestAlgorithm='" + digestAlgorithm + '\'' +
               '}';
    }

    @JsonIgnore
    public Digest toDigest()
    {
        if (digestAlgorithm.equalsIgnoreCase(MD5Digest.MD5_ALGORITHM))
        {
            return new MD5Digest(digest);
        }
        else if (digestAlgorithm.equalsIgnoreCase(XXHash32Digest.XXHASH_32_ALGORITHM))
        {
            return new XXHash32Digest(digest);
        }

        throw new IllegalArgumentException("Digest algorithm " + digestAlgorithm + " is unknown");
    }
}
