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

import java.util.UUID;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.common.data.RestoreJobSecrets;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.apache.cassandra.sidecar.foundation.RestoreJobSecretsGen;

import static org.assertj.core.api.Assertions.assertThat;

class StorageClientPoolTest
{
    @Test
    void testRetrieveClient() throws RestoreJobFatalException
    {
        StorageClientPool pool = new StorageClientPool(new SidecarConfigurationImpl(), null);
        RestoreJob job = RestoreJob.builder()
                                   .jobId(UUID.randomUUID())
                                   .jobSecrets(RestoreJobSecretsGen.genRestoreJobSecrets())
                                   .build();
        StorageClient client = pool.storageClient(job);
        assertThat(client).isNotNull();
    }

    @Test
    void testRetrieveClientWithIamCredentials() throws RestoreJobFatalException
    {
        StorageClientPool pool = new StorageClientPool(new SidecarConfigurationImpl(), null);
        RestoreJobSecrets iamSecrets = RestoreJobSecrets.iamMode("us-east-1");
        RestoreJob job = RestoreJob.builder()
                                   .jobId(UUID.randomUUID())
                                   .jobSecrets(iamSecrets)
                                   .build();
        StorageClient client = pool.storageClient(job);
        assertThat(client).isNotNull();
    }

    @Test
    void testIamJobsInSameRegionShareStorageClient() throws RestoreJobFatalException
    {
        StorageClientPool pool = new StorageClientPool(new SidecarConfigurationImpl(), null);
        RestoreJobSecrets iamSecrets = RestoreJobSecrets.iamMode("us-east-1");

        RestoreJob job1 = RestoreJob.builder().jobId(UUID.randomUUID()).jobSecrets(iamSecrets).build();
        RestoreJob job2 = RestoreJob.builder().jobId(UUID.randomUUID()).jobSecrets(iamSecrets).build();

        StorageClient client1 = pool.storageClient(job1);
        StorageClient client2 = pool.storageClient(job2);

        assertThat(client1).isSameAs(client2);
    }

    @Test
    void testIamJobsInDifferentRegionsGetDifferentStorageClients() throws RestoreJobFatalException
    {
        StorageClientPool pool = new StorageClientPool(new SidecarConfigurationImpl(), null);

        RestoreJob job1 = RestoreJob.builder()
                                    .jobId(UUID.randomUUID())
                                    .jobSecrets(RestoreJobSecrets.iamMode("us-east-1"))
                                    .build();
        RestoreJob job2 = RestoreJob.builder()
                                    .jobId(UUID.randomUUID())
                                    .jobSecrets(RestoreJobSecrets.iamMode("eu-west-1"))
                                    .build();

        StorageClient client1 = pool.storageClient(job1);
        StorageClient client2 = pool.storageClient(job2);

        assertThat(client1).isNotSameAs(client2);
    }
}
