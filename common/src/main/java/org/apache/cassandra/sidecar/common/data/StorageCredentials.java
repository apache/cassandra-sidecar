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
import com.fasterxml.jackson.annotation.JsonProperty;

import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.CREDENTIALS_ACCESS_KEY_ID;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.CREDENTIALS_REGION;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.CREDENTIALS_SECRET_ACCESS_KEY;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.CREDENTIALS_SESSION_TOKEN;

/**
 * Used for storing credential details needed for either reading/writing to S3
 */
public class StorageCredentials
{
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String sessionToken;
    private final String region;

    public static Builder builder()
    {
        return new Builder();
    }

    @JsonCreator
    public StorageCredentials(@JsonProperty(CREDENTIALS_ACCESS_KEY_ID) String accessKeyId,
                              @JsonProperty(CREDENTIALS_SECRET_ACCESS_KEY) String secretAccessKey,
                              @JsonProperty(CREDENTIALS_SESSION_TOKEN) String sessionToken,
                              @JsonProperty(CREDENTIALS_REGION) String region)
    {
        Objects.requireNonNull(accessKeyId, "accessKeyId must be supplied");
        Objects.requireNonNull(secretAccessKey, "secretAccessKey must be supplied");
        Objects.requireNonNull(sessionToken, "sessionToken must be supplied");
        Objects.requireNonNull(region, "region must be supplied");
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.sessionToken = sessionToken;
        this.region = region;
    }

    @JsonProperty(CREDENTIALS_ACCESS_KEY_ID)
    public String accessKeyId()
    {
        return accessKeyId;
    }

    @JsonProperty(CREDENTIALS_SECRET_ACCESS_KEY)
    public String secretAccessKey()
    {
        return secretAccessKey;
    }

    @JsonProperty(CREDENTIALS_SESSION_TOKEN)
    public String sessionToken()
    {
        return sessionToken;
    }

    @JsonProperty(CREDENTIALS_REGION)
    public String region()
    {
        return region;
    }

    /**
     * @return secrets string with redacted values
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
     * Builds the RestoreJobSecrets
     */
    public static class Builder
    {
        private String accessKeyId;
        private String secretAccessKey;
        private String sessionToken;
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
