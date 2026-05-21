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

package org.apache.cassandra.sidecar.common.data;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.CREDENTIALS_ACCESS_KEY_ID;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.CREDENTIALS_REGION;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.CREDENTIALS_SECRET_ACCESS_KEY;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.CREDENTIALS_SESSION_TOKEN;

/**
 * Used for storing credential details needed for either reading/writing to S3
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class StorageCredentials
{
    private final @Nullable String accessKeyId;
    private final @Nullable String secretAccessKey;
    private final @Nullable String sessionToken;
    private final String region;

    public static Builder builder()
    {
        return new Builder();
    }

    @JsonCreator
    public StorageCredentials(@JsonProperty(CREDENTIALS_ACCESS_KEY_ID) @Nullable String accessKeyId,
                              @JsonProperty(CREDENTIALS_SECRET_ACCESS_KEY) @Nullable String secretAccessKey,
                              @JsonProperty(CREDENTIALS_SESSION_TOKEN) @Nullable String sessionToken,
                              @JsonProperty(CREDENTIALS_REGION) String region)
    {
        Objects.requireNonNull(region, "region must be supplied");
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.sessionToken = sessionToken;
        this.region = region;
    }

    @JsonProperty(CREDENTIALS_ACCESS_KEY_ID)
    public @Nullable String accessKeyId()
    {
        return accessKeyId;
    }

    @JsonProperty(CREDENTIALS_SECRET_ACCESS_KEY)
    public @Nullable String secretAccessKey()
    {
        return secretAccessKey;
    }

    @JsonProperty(CREDENTIALS_SESSION_TOKEN)
    public @Nullable String sessionToken()
    {
        return sessionToken;
    }

    @JsonProperty(CREDENTIALS_REGION)
    public String region()
    {
        return region;
    }

    /**
     * Returns true if all three key fields ({@code accessKeyId}, {@code secretAccessKey}, {@code sessionToken})
     * are non-null, indicating that static AWS credentials are present and should be used.
     *
     * <p>All three fields are required together because the sidecar exclusively uses
     * {@code AwsSessionCredentials} — STS-issued short-lived
     * credentials that include a session token. Long-term IAM user keys (key + secret without a token)
     * are intentionally unsupported: they cannot expire, carry broader permissions, and pose a higher
     * blast radius if leaked. Callers that want permanent access should use IAM instance profiles instead.
     *
     * <p>A return value of {@code false} means the key fields are absent. Callers that have already validated
     * credentials via {@link org.apache.cassandra.sidecar.common.request.data.CreateRestoreJobRequestPayload}
     * can treat this as an indication to use the AWS default credential chain (IAM instance profile, etc.).
     * Callers that have not performed that validation should not assume {@code false} implies a valid IAM state —
     * it could also mean partially-populated credentials that were not yet rejected.
     */
    public boolean hasStaticCredentials()
    {
        return accessKeyId != null && secretAccessKey != null && sessionToken != null;
    }

    /**
     * @return credentials string with all secret values redacted; safe for logging
     */
    @Override
    public String toString()
    {
        return String.format("StorageCredentials{%s=%s, %s=%s, %s=%s, %s=%s}",
                             CREDENTIALS_ACCESS_KEY_ID, accessKeyId,
                             CREDENTIALS_SECRET_ACCESS_KEY, "redacted",
                             CREDENTIALS_SESSION_TOKEN, "redacted",
                             CREDENTIALS_REGION, region);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;

        if (!(obj instanceof StorageCredentials))
            return false;

        StorageCredentials that = (StorageCredentials) obj;
        return Objects.equals(this.accessKeyId, that.accessKeyId)
               && Objects.equals(this.secretAccessKey, that.secretAccessKey)
               && Objects.equals(this.sessionToken, that.sessionToken)
               && Objects.equals(this.region, that.region);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(accessKeyId, secretAccessKey, sessionToken, region);
    }

    /**
     * Builds a {@link StorageCredentials} instance
     */
    public static class Builder
    {
        private @Nullable String accessKeyId;
        private @Nullable String secretAccessKey;
        private @Nullable String sessionToken;
        private String region;

        private Builder()
        {
        }

        public Builder accessKeyId(String accessKeyId)
        {
            this.accessKeyId = accessKeyId;
            return this;
        }

        public Builder secretAccessKey(String secretAccessKey)
        {
            this.secretAccessKey = secretAccessKey;
            return this;
        }

        public Builder sessionToken(String sessionToken)
        {
            this.sessionToken = sessionToken;
            return this;
        }

        public Builder region(String region)
        {
            this.region = region;
            return this;
        }

        public StorageCredentials build()
        {
            return new StorageCredentials(accessKeyId, secretAccessKey, sessionToken, region);
        }
    }
}
