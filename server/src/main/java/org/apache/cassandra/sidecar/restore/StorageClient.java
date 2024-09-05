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
import java.nio.channels.Channel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.SidecarRateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Future;
import org.apache.cassandra.sidecar.common.data.StorageCredentials;
import org.apache.cassandra.sidecar.common.server.utils.ThrowableUtils;
import org.apache.cassandra.sidecar.common.utils.HttpRange;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.config.yaml.S3ClientConfigurationImpl;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreRange;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * The client to access an S3-compatible storage service
 */
public class StorageClient
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StorageClient.class);

    private final S3AsyncClient client;
    private final int rangeHeaderSize;
    private final SidecarRateLimiter downloadRateLimiter;
    private final Map<UUID, Credentials> credentialsProviders = new ConcurrentHashMap<>();

    @VisibleForTesting
    StorageClient(S3AsyncClient client)
    {
        // no rate-limiting
        this(client,
             S3ClientConfigurationImpl.DEFAULT_RANGE_GET_OBJECT_BYTES_SIZE,
             SidecarRateLimiter.create(-1));
    }

    StorageClient(S3AsyncClient client, int rangeHeaderSize, SidecarRateLimiter downloadRateLimiter)
    {
        this.client = client;
        this.rangeHeaderSize = rangeHeaderSize;
        this.downloadRateLimiter = downloadRateLimiter;
    }

    /**
     * Authenticate and cache the credentials of a {@link RestoreJob}
     */
    public StorageClient authenticate(RestoreJob restoreJob) throws RestoreJobFatalException
    {
        Credentials newCredentials = new Credentials(restoreJob);
        // Update the credential if absent or secrets is outdated.
        credentialsProviders.compute(restoreJob.jobId, (jobId, credentials) -> {
            if (credentials == null || !matches(credentials, newCredentials))
            {
                LOGGER.info("Credentials are updated in the storage client. jobId={} credentials={}",
                            restoreJob.jobId, newCredentials.readCredentials);
                newCredentials.init();
                return newCredentials;
            }
            else
            {
                return credentials;
            }
        });
        return this;
    }

    /**
     * Revoke the credentials of a {@link RestoreJob}
     * It should be called when the job is in a final {@link org.apache.cassandra.sidecar.common.data.RestoreJobStatus}
     *
     * @param jobId the unique identifier for the job
     */
    public void revokeCredentials(UUID jobId)
    {
        LOGGER.info("Revoke credentials for job. jobId={}", jobId);
        credentialsProviders.remove(jobId);
    }

    /**
     * Check object existence with matching checksum
     * @param range restore range
     * @return future of HeadObjectResponse
     */
    public CompletableFuture<HeadObjectResponse> objectExists(RestoreRange range)
    {
        Credentials credentials = credentialsProviders.get(range.jobId());
        if (credentials == null)
        {
            LOGGER.warn("Credentials not found. jobId={}", range.jobId());
            return failedFuture(credentialsNotFound(range));
        }

        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html
        HeadObjectRequest request =
        HeadObjectRequest.builder()
                         .overrideConfiguration(b -> b.credentialsProvider(credentials.awsCredentialsProvider()))
                         .bucket(range.sliceBucket())
                         .key(range.sliceKey())
                         .ifMatch(quoteIfNeeded(range.sliceChecksum()))
                         .build();

        return client.headObject(request)
                     .whenComplete(logCredentialOnRequestFailure(range, credentials));
    }

    public Future<File> downloadObjectIfAbsent(RestoreRange range, TaskExecutorPool taskExecutorPool)
    {
        Credentials credentials = credentialsProviders.get(range.jobId());
        if (credentials == null)
        {
            LOGGER.warn("Credentials to download object not found. jobId={}", range.jobId());
            return Future.failedFuture(credentialsNotFound(range));
        }

        Path objectPath = range.stagedObjectPath();
        File object = objectPath.toFile();
        if (object.exists())
        {
            LOGGER.info("Skipping download, file already exists. jobId={} sliceKey={}",
                        range.jobId(), range.sliceKey());
            // Skip downloading if the file already exists on disk. It should be a rare scenario.
            // Note that the on-disk file could be different from the remote object, although the name matches.
            // TODO 1: verify etag does not change after s3 replication and batch copy
            // TODO 2: extend restore_job table to define the multi-part upload chunk size, in order to perform local
            //         verification of the etag/checksum
            // For now, we just skip download, assuming the scenario is rare and no maliciousness
            return Future.succeededFuture(object);
        }

        try
        {
            Files.createDirectories(objectPath.getParent());
        }
        catch (Exception ex)
        {
            LOGGER.error("Error occurred while creating directory. jobId={} sliceKey={}",
                         range.jobId(), range.sliceKey(), ex);
            return Future.failedFuture(ex);
        }

        LOGGER.info("Downloading object. jobId={} sliceKey={}", range.jobId(), range.sliceKey());
        return rangeGetObject(range, credentials, objectPath, taskExecutorPool);
    }

    public void close()
    {
        try
        {
            client.close(); // it closes the thread pool internally
        }
        catch (Exception ex)
        {
            LOGGER.warn("Error when closing", ex);
        }
    }

    // Range-GetObject with http range header
    private Future<File> rangeGetObject(RestoreRange range, Credentials credentials, Path destinationPath, TaskExecutorPool taskExecutorPool)
    {
        HttpRangesIterator iterator = new HttpRangesIterator(range.sliceObjectLength(), rangeHeaderSize);
        Preconditions.checkState(iterator.hasNext(), "SliceObject is empty. sliceKey=" + range.sliceKey());

        SeekableByteChannel seekableByteChannel;
        try
        {
            seekableByteChannel = Files.newByteChannel(destinationPath, EnumSet.of(CREATE_NEW, WRITE));
        }
        catch (IOException e)
        {
            LOGGER.error("Failed to create file channel for downloading. jobId={} sliceKey={}",
                         range.jobId(), range.sliceKey(), e);
            return Future.failedFuture(e);
        }
        Future<SeekableByteChannel> channelFuture = Future.succeededFuture(seekableByteChannel);
        while (iterator.hasNext())
        {
            HttpRange httpRange = iterator.next();
            // Schedule each part download in the taskExecutorPool with ordered == true.
            // Parts are downloaded one by one in sequence.
            channelFuture = channelFuture.compose(channel -> taskExecutorPool.executeBlocking(() -> {
                // the length is guaranteed to be no greater than rangeHeaderSize (int)
                int actualRangeSize = (int) httpRange.length();
                // throttle the download throughput
                downloadRateLimiter.acquire(actualRangeSize);
                // https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
                GetObjectRequest request =
                GetObjectRequest.builder()
                                .overrideConfiguration(b -> b.credentialsProvider(credentials.awsCredentialsProvider()))
                                .bucket(range.sliceBucket())
                                .key(range.sliceKey())
                                .range(httpRange.toString())
                                .build();
                // note: it is a blocking get; No parallelism in getting the ranges of the same object
                ResponseBytes<GetObjectResponse> bytes = client.getObject(request, AsyncResponseTransformer.toBytes()).get();
                channel.write(bytes.asByteBuffer());
                return channel;
            }, true));
        }
        return channelFuture
               // eventually is evaluated in both success and failure cases
               .eventually(() -> taskExecutorPool.runBlocking(() -> {
                   ThrowableUtils.propagate(() -> closeChannel(seekableByteChannel));
               }, true))
               .compose(channel -> Future.succeededFuture(destinationPath.toFile()),
                        failure -> { // failure mapper; log the credential on failure
                            LOGGER.error("Request is not successful. jobId={} credentials={}",
                                         range.jobId(), credentials.readCredentials, failure);
                            try
                            {
                                Files.deleteIfExists(destinationPath);
                            }
                            catch (IOException e)
                            {
                                LOGGER.warn("Failed to clean up the failed download. jobId={} sliceKey={}",
                                            range.jobId(), range.sliceKey(), e);
                                failure.addSuppressed(e);
                            }
                            return Future.failedFuture(failure);
                        });
    }

    private boolean matches(Credentials c1, Credentials c2)
    {
        if (c1 == c2)
            return true;

        return Objects.equals(c1.readCredentials, c2.readCredentials);
    }

    private String quoteIfNeeded(String input)
    {
        if (input.startsWith("\"") && input.endsWith("\""))
            return input;
        return '"' + input + '"';
    }

    private IllegalStateException credentialsNotFound(RestoreRange range)
    {
        return new IllegalStateException("No credential available. The job might already have failed." +
                                         "jobId: " + range.jobId());
    }

    private BiConsumer<Object, ? super Throwable> logCredentialOnRequestFailure(RestoreRange range,
                                                                                Credentials credentials)
    {
        return (ignored, cause) -> {
            if (cause != null)
            {
                LOGGER.error("Request is not successful. jobId={} credentials={}",
                             range.jobId(), credentials.readCredentials, cause);
            }
        };
    }

    /**
     * Closes the channel if not-null and open
     *
     * @param channel the channel to be closed
     */
    private void closeChannel(Channel channel) throws IOException
    {
        if (channel != null && channel.isOpen())
        {
            channel.close();
        }
    }

    private static <T> CompletableFuture<T> failedFuture(Throwable cause)
    {
        CompletableFuture<T> failure = new CompletableFuture<>();
        failure.completeExceptionally(cause);
        return failure;
    }

    private static class Credentials
    {
        final StorageCredentials readCredentials;
        private AwsCredentialsProvider awsCredential;

        Credentials(RestoreJob restoreJob) throws RestoreJobFatalException
        {
            if (restoreJob.secrets == null)
            {
                throw new RestoreJobFatalException("Restore job is missing credentials. JobId: " + restoreJob.jobId);
            }

            this.readCredentials = restoreJob.secrets.readCredentials();
        }

        void init()
        {
            AwsCredentials credentials = AwsSessionCredentials.create(readCredentials.accessKeyId(),
                                                                      readCredentials.secretAccessKey(),
                                                                      readCredentials.sessionToken());
            this.awsCredential = StaticCredentialsProvider.create(credentials);
        }

        AwsCredentialsProvider awsCredentialsProvider()
        {
            return awsCredential;
        }
    }
}
