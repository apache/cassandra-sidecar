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
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
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

import org.apache.cassandra.sidecar.common.data.StorageCredentials;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreSlice;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.apache.cassandra.sidecar.exceptions.ThrowableUtils;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.ResponsePublisher;
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
    private final SidecarRateLimiter downloadRateLimiter;
    private final Map<UUID, Credentials> credentialsProviders = new ConcurrentHashMap<>();

    @VisibleForTesting
    StorageClient(S3AsyncClient client)
    {
        // no rate-limiting
        this(client, SidecarRateLimiter.create(-1));
    }

    StorageClient(S3AsyncClient client, SidecarRateLimiter downloadRateLimiter)
    {
        this.client = client;
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

    public CompletableFuture<HeadObjectResponse> objectExists(RestoreSlice slice)
    {
        Credentials credentials = credentialsProviders.get(slice.jobId());
        if (credentials == null)
        {
            CompletableFuture<HeadObjectResponse> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(credentialsNotFound(slice));
            return failedFuture;
        }

        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html
        HeadObjectRequest request =
        HeadObjectRequest.builder()
                         .overrideConfiguration(b -> b.credentialsProvider(credentials.awsCredentialsProvider()))
                         .bucket(slice.bucket())
                         .key(slice.key())
                         .ifMatch(quoteIfNeeded(slice.checksum()))
                         .build();

        return client.headObject(request)
                     .whenComplete(logCredentialOnRequestFailure(slice, credentials));
    }

    public CompletableFuture<File> downloadObjectIfAbsent(RestoreSlice slice)
    {
        Credentials credentials = credentialsProviders.get(slice.jobId());
        if (credentials == null)
        {
            LOGGER.warn("Credentials to download object not found. jobId={}", slice.jobId());
            CompletableFuture<File> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(credentialsNotFound(slice));
            return failedFuture;
        }

        Path objectPath = slice.stagedObjectPath();
        File object = objectPath.toFile();
        if (object.exists())
        {
            LOGGER.info("Skipping download, file already exists. jobId={} sliceKey={}",
                         slice.jobId(), slice.key());
            // Skip downloading if the file already exists on disk. It should be a rare scenario.
            // Note that the on-disk file could be different from the remote object, although the name matches.
            // TODO 1: verify etag does not change after s3 replication and batch copy
            // TODO 2: extend restore_job table to define the multi-part upload chunk size, in order to perform local
            //         verification of the etag/checksum
            // For now, we just skip download, assuming the scenario is rare and no maliciousness
            return CompletableFuture.completedFuture(object);
        }

        if (!object.getParentFile().mkdirs())
        {
            LOGGER.warn("Error occurred while creating directory. jobId={} sliceKey={}",
                        slice.jobId(), slice.key());

        }

        LOGGER.info("Downloading object. jobId={} sliceKey={}", slice.jobId(), slice.key());
        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
        GetObjectRequest request =
        GetObjectRequest.builder()
                        .overrideConfiguration(b -> b.credentialsProvider(credentials.awsCredentialsProvider()))
                        .bucket(slice.bucket())
                        .key(slice.key())
                        .build();
        return rateLimitedGetObject(slice, client, request, objectPath)
               .whenComplete(logCredentialOnRequestFailure(slice, credentials))
               .thenApply(res -> object);
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

    private IllegalStateException credentialsNotFound(RestoreSlice slice)
    {
        return new IllegalStateException("No credential available. The job might already have failed." +
                                         "jobId: " + slice.jobId());
    }

    private BiConsumer<Object, ? super Throwable> logCredentialOnRequestFailure(RestoreSlice slice,
                                                                                Credentials credentials)
    {
        return (ignored, cause) -> {
            if (cause != null)
            {
                LOGGER.error("GetObjectRequest is not successful. jobId={} credentials={}",
                             slice.jobId(), credentials.readCredentials, cause);
            }
        };
    }

    /**
     * Returns a {@link CompletableFuture} to the {@link GetObjectResponse}. It writes the object from S3 to a file
     * applying rate limiting on the download throughput.
     *
     * @param slice           the slice to be restored
     * @param client          the S3 client
     * @param request         the {@link GetObjectRequest request}
     * @param destinationPath the path where the object will be persisted
     * @return a {@link CompletableFuture} of the {@link GetObjectResponse}
     */
    private CompletableFuture<GetObjectResponse> rateLimitedGetObject(RestoreSlice slice,
                                                                      S3AsyncClient client,
                                                                      GetObjectRequest request,
                                                                      Path destinationPath)
    {
        return client.getObject(request, AsyncResponseTransformer.toPublisher())
                     .thenCompose(responsePublisher -> subscribeRateLimitedWrite(slice,
                                                                                 destinationPath,
                                                                                 responsePublisher));
    }

    /**
     * Returns a {@link CompletableFuture} to the {@link GetObjectResponse} and consuming the GetObjectResponse
     * by subscribing to the {@code publisher}. Applying backpressure on the received bytes by rate limiting
     * the download throughput using the {@code downloadRateLimiter} object.
     *
     * @param slice           the slice to be restored
     * @param destinationPath the path where the object will be persisted
     * @param publisher       the {@link ResponsePublisher}
     * @return a {@link CompletableFuture} to the {@link GetObjectResponse}
     */
    CompletableFuture<GetObjectResponse> subscribeRateLimitedWrite(RestoreSlice slice,
                                                                   Path destinationPath,
                                                                   ResponsePublisher<GetObjectResponse> publisher)
    {
        WritableByteChannel channel;
        try
        {
            // always create new file, and fails if it already exists
            // this is consistent with the expectation that we won't
            // re-download a file that already exists
            // The channel is closed on completion of streaming asynchronously
            channel = Files.newByteChannel(destinationPath, EnumSet.of(CREATE_NEW, WRITE));
        }
        catch (FileAlreadyExistsException fileAlreadyExistsException)
        {
            LOGGER.info("Skipping download. File already exists. jobId={} sliceKey={}",
                         slice.jobId(), slice.key());
            return CompletableFuture.completedFuture(publisher.response());
        }
        catch (IOException e)
        {
            LOGGER.error("Error occurred while creating channel. destinationPath={} jobId={} sliceKey={}",
                         destinationPath, slice.jobId(), slice.key(), e);
            throw new RuntimeException(e);
        }
        // CompletableFuture that will be notified when all events have been consumed or if an error occurs.
        return publisher
               .subscribe(buffer -> {
                   downloadRateLimiter.acquire(buffer.remaining()); // apply backpressure on the received bytes
                   ThrowableUtils.propagate(() -> channel.write(buffer));
               })
               .whenComplete((v, subscribeThrowable) -> {
                   // finally close the channel and log error if failed
                   closeChannel(channel);

                   if (subscribeThrowable != null)
                   {
                       LOGGER.error("Error occurred while downloading. jobId={} sliceKey={}",
                                    slice.jobId(), slice.key(), subscribeThrowable);
                   }
               })
               .thenApply(v -> publisher.response());
    }

    /**
     * Closes the channel if not-null. Wraps any {@link IOException} in a {@link RuntimeException}
     *
     * @param channel the channel to be closed
     * @throws RuntimeException wrapping any {@link IOException}
     */
    private void closeChannel(Channel channel)
    {
        if (channel != null && channel.isOpen())
        {
            try
            {
                channel.close();
            }
            catch (IOException e)
            {
                LOGGER.error("Failed to close channel", e);
            }
        }
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
