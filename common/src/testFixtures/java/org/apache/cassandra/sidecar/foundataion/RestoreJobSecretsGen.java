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

package org.apache.cassandra.sidecar.foundataion;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.cassandra.sidecar.common.data.RestoreJobSecrets;
import org.apache.cassandra.sidecar.common.data.StorageCredentials;

/**
 * Generator for {@link StorageCredentials} for testing
 */
public class RestoreJobSecretsGen
{
    private RestoreJobSecretsGen() {}

    public static StorageCredentials genReadStorageCredentials()
    {
        return genStorageCredentials("read");
    }

    public static StorageCredentials genWriteStorageCredentials()
    {
        return genStorageCredentials("write");
    }

    public static RestoreJobSecrets genRestoreJobSecrets()
    {
        return new RestoreJobSecrets(genReadStorageCredentials(), genWriteStorageCredentials());
    }

    private static StorageCredentials genStorageCredentials(String permission)
    {
        return StorageCredentials
               .builder()
               .accessKeyId(permission + "-accessKeyId-" + ThreadLocalRandom.current().nextLong())
               .secretAccessKey(permission + "-secretAccessKey-" + ThreadLocalRandom.current().nextLong())
               .sessionToken(permission + "-sessionToken-" + ThreadLocalRandom.current().nextLong())
               .region(permission + "-region-" + ThreadLocalRandom.current().nextLong())
               .build();
    }
}
