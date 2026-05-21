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
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import org.apache.cassandra.sidecar.common.data.RestoreJobSecrets;
import org.apache.cassandra.sidecar.common.data.StorageCredentials;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreRange;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.apache.cassandra.sidecar.foundation.RestoreJobSecretsGen;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that {@link StorageClient} selects the correct AWS credential provider based on the restore job's
 * credentials.
 *
 * <p>Two modes are exercised:
 * <ul>
 *   <li>Static credentials: all three key fields present → {@link StaticCredentialsProvider}</li>
 *   <li>IAM instance profile: only region present → {@link DefaultCredentialsProvider}</li>
 * </ul>
 *
 * <p>The provider type is verified by capturing the {@link HeadObjectRequest} passed to the mocked
 * {@link S3AsyncClient} and inspecting the {@code overrideConfiguration.credentialsProvider}.
 * This works because {@link StorageClient} injects the per-job provider at request build time
 * (see {@code objectExists} and {@code rangeGetObject}).
 */
class StorageClientCredentialsTest
{
    private S3AsyncClient mockS3;
    private ArgumentCaptor<HeadObjectRequest> requestCaptor;

    @BeforeEach
    void setUp()
    {
        mockS3 = mock(S3AsyncClient.class);
        requestCaptor = ArgumentCaptor.forClass(HeadObjectRequest.class);
        when(mockS3.headObject(requestCaptor.capture()))
            .thenReturn(CompletableFuture.completedFuture(HeadObjectResponse.builder().build()));
    }

    @Test
    void testStaticCredentialsSelectsStaticProvider() throws RestoreJobFatalException
    {
        UUID jobId = UUIDs.timeBased();
        RestoreJob job = RestoreJob.builder()
                                   .jobId(jobId)
                                   .jobSecrets(RestoreJobSecretsGen.genRestoreJobSecrets())
                                   .build();
        new StorageClient(mockS3).authenticate(job).objectExists(mockRange(jobId));

        assertThat(captureCredentialProvider()).isInstanceOf(StaticCredentialsProvider.class);
    }

    @Test
    void testIamModeSelectsDefaultCredentialsProvider() throws RestoreJobFatalException
    {
        UUID jobId = UUIDs.timeBased();
        // Region-only credentials signal IAM mode; no accessKeyId/secretAccessKey/sessionToken
        StorageCredentials regionOnly = StorageCredentials.builder().region("us-east-1").build();
        RestoreJob job = RestoreJob.builder()
                                   .jobId(jobId)
                                   .jobSecrets(new RestoreJobSecrets(regionOnly, regionOnly))
                                   .build();
        new StorageClient(mockS3).authenticate(job).objectExists(mockRange(jobId));

        assertThat(captureCredentialProvider()).isInstanceOf(DefaultCredentialsProvider.class);
    }

    @Test
    void testIamJobsShareSingletonDefaultProvider() throws RestoreJobFatalException
    {
        // All IAM-mode jobs must share the same DefaultCredentialsProvider instance.
        // DefaultCredentialsProvider.create() initializes a thread pool for async refresh;
        // creating one per job would waste resources and is unnecessary.
        StorageClient client = new StorageClient(mockS3);
        StorageCredentials regionOnly = StorageCredentials.builder().region("us-east-1").build();

        UUID jobId1 = UUIDs.timeBased();
        client.authenticate(RestoreJob.builder()
                                      .jobId(jobId1)
                                      .jobSecrets(new RestoreJobSecrets(regionOnly, regionOnly))
                                      .build());
        client.objectExists(mockRange(jobId1));
        AwsCredentialsProvider first = captureCredentialProvider();

        UUID jobId2 = UUIDs.timeBased();
        client.authenticate(RestoreJob.builder()
                                      .jobId(jobId2)
                                      .jobSecrets(new RestoreJobSecrets(regionOnly, regionOnly))
                                      .build());
        client.objectExists(mockRange(jobId2));
        AwsCredentialsProvider second = captureCredentialProvider();

        assertThat(second).isSameAs(first);
        assertThat(first).isInstanceOf(DefaultCredentialsProvider.class);
    }

    @Test
    void testIamJobsShareSingletonDefaultProviderAcrossClientInstances() throws RestoreJobFatalException
    {
        // The singleton must be shared across separate StorageClient instances (different regions
        // in the pool each get their own StorageClient), not just within one client.
        StorageCredentials regionOnly = StorageCredentials.builder().region("us-east-1").build();
        UUID jobId1 = UUIDs.timeBased();
        UUID jobId2 = UUIDs.timeBased();

        S3AsyncClient mockS3b = mock(S3AsyncClient.class);
        ArgumentCaptor<HeadObjectRequest> captorB = ArgumentCaptor.forClass(HeadObjectRequest.class);
        when(mockS3b.headObject(captorB.capture()))
            .thenReturn(CompletableFuture.completedFuture(HeadObjectResponse.builder().build()));

        new StorageClient(mockS3).authenticate(RestoreJob.builder()
                                                         .jobId(jobId1)
                                                         .jobSecrets(new RestoreJobSecrets(regionOnly, regionOnly))
                                                         .build())
                                 .objectExists(mockRange(jobId1));
        AwsCredentialsProvider first = captureCredentialProvider();

        new StorageClient(mockS3b).authenticate(RestoreJob.builder()
                                                          .jobId(jobId2)
                                                          .jobSecrets(new RestoreJobSecrets(regionOnly, regionOnly))
                                                          .build())
                                  .objectExists(mockRange(jobId2));
        AwsCredentialsProvider second = captorB.getValue()
                                               .overrideConfiguration()
                                               .flatMap(c -> c.credentialsProvider())
                                               .orElseThrow(() -> new AssertionError("No credential provider on second request"));

        assertThat(second).isSameAs(first);
        assertThat(first).isInstanceOf(DefaultCredentialsProvider.class);
    }

    @Test
    void testCredentialsAreReplacedWhenSecretsChange() throws RestoreJobFatalException
    {
        StorageClient client = new StorageClient(mockS3);
        UUID jobId = UUIDs.timeBased();

        RestoreJob job1 = RestoreJob.builder()
                                    .jobId(jobId)
                                    .jobSecrets(RestoreJobSecretsGen.genRestoreJobSecrets())
                                    .build();
        client.authenticate(job1);
        client.objectExists(mockRange(jobId));
        AwsCredentialsProvider first = captureCredentialProvider();

        // Re-authenticate with different static credentials (new random keys).
        // The credentials are guaranteed to differ because genRestoreJobSecrets uses random values.
        RestoreJob job2 = RestoreJob.builder()
                                    .jobId(jobId)
                                    .jobSecrets(RestoreJobSecretsGen.genRestoreJobSecrets())
                                    .build();
        client.authenticate(job2);
        client.objectExists(mockRange(jobId));
        AwsCredentialsProvider second = captureCredentialProvider();

        // The provider instances must differ, confirming the old credentials were replaced
        assertThat(second).isNotSameAs(first);
        assertThat(second).isInstanceOf(StaticCredentialsProvider.class);
    }

    @Test
    void testObjectExistsWithoutAuthenticateFails()
    {
        // objectExists before any authenticate() call must return a failed future with a clear error,
        // not a NullPointerException or other unexpected runtime exception.
        UUID jobId = UUIDs.timeBased();
        CompletableFuture<HeadObjectResponse> result = new StorageClient(mockS3).objectExists(mockRange(jobId));
        assertThat(result).isCompletedExceptionally();
        assertThatThrownBy(result::join)
            .hasCauseInstanceOf(IllegalStateException.class)
            .hasMessageContaining("No credential available");
    }

    @Test
    void testCredentialsAreNotReplacedWhenSecretsUnchanged() throws RestoreJobFatalException
    {
        // When authenticate is called twice with identical secrets, the cached provider must not change.
        // This avoids redundant StaticCredentialsProvider re-creation on every poll cycle.
        StorageClient client = new StorageClient(mockS3);
        UUID jobId = UUIDs.timeBased();
        RestoreJobSecrets secrets = RestoreJobSecretsGen.genRestoreJobSecrets();

        RestoreJob job = RestoreJob.builder().jobId(jobId).jobSecrets(secrets).build();
        client.authenticate(job);
        client.objectExists(mockRange(jobId));
        AwsCredentialsProvider first = captureCredentialProvider();

        // Authenticate again with an equal (but not same) RestoreJob — secrets are equal so no replacement
        client.authenticate(RestoreJob.builder().jobId(jobId).jobSecrets(secrets).build());
        client.objectExists(mockRange(jobId));
        AwsCredentialsProvider second = captureCredentialProvider();

        assertThat(second).isSameAs(first);
    }

    private RestoreRange mockRange(UUID jobId)
    {
        RestoreRange range = mock(RestoreRange.class);
        when(range.jobId()).thenReturn(jobId);
        when(range.sliceBucket()).thenReturn("test-bucket");
        when(range.sliceKey()).thenReturn("test-key");
        when(range.sliceChecksum()).thenReturn("\"abc123\"");
        return range;
    }

    private AwsCredentialsProvider captureCredentialProvider()
    {
        HeadObjectRequest captured = requestCaptor.getValue();
        return captured.overrideConfiguration()
                       .flatMap(c -> c.credentialsProvider())
                       .orElseThrow(() -> new AssertionError("No credential provider set on request"));
    }
}
