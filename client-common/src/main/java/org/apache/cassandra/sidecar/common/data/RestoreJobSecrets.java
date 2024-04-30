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

import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.SECRET_READ_CREDENTIALS;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.SECRET_WRITE_CREDENTIALS;

/**
 * Holds both read and write credentials needed for talk to S3
 */
public class RestoreJobSecrets
{
    private final StorageCredentials writeCredentials;
    private final StorageCredentials readCredentials;

    @JsonCreator
    public RestoreJobSecrets(@JsonProperty(SECRET_READ_CREDENTIALS) StorageCredentials readCredentials,
                             @JsonProperty(SECRET_WRITE_CREDENTIALS) StorageCredentials writeCredentials)
    {
        Objects.requireNonNull(readCredentials, "Read credentials must be supplied");
        Objects.requireNonNull(writeCredentials, "Write credentials must be supplied");
        this.readCredentials = readCredentials;
        this.writeCredentials = writeCredentials;
    }

    /**
     * @return credentials that have write permission
     */
    @JsonProperty(SECRET_WRITE_CREDENTIALS)
    public StorageCredentials writeCredentials()
    {
        return writeCredentials;
    }

    /**
     * @return credentials that have read permission
     */
    @JsonProperty(SECRET_READ_CREDENTIALS)
    public StorageCredentials readCredentials()
    {
        return readCredentials;
    }

    @Override
    public String toString()
    {
        return String.format("RestoreJobSecrets{%s=%s, %s=%s}",
                             SECRET_WRITE_CREDENTIALS, writeCredentials,
                             SECRET_READ_CREDENTIALS, readCredentials);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;

        if (!(obj instanceof RestoreJobSecrets))
            return false;

        RestoreJobSecrets that = (RestoreJobSecrets) obj;
        return Objects.equals(this.readCredentials, that.readCredentials)
               && Objects.equals(this.writeCredentials, that.writeCredentials);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(writeCredentials, readCredentials);
    }
}
