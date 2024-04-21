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

package org.apache.cassandra.sidecar.utils;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.TableOperations;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.ServiceConfiguration;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetrics;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * This class is in charge of performing SSTable imports into the desired Cassandra instance.
 * Since imports are synchronized in the Cassandra side on a per table-basis, we only perform one import per
 * Cassandra instance's keyspace/table, and we queue the rest of the import requests.
 */
@Singleton
public class SSTableImporter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SSTableImporter.class);
    public static final boolean DEFAULT_RESET_LEVEL = true;
    public static final boolean DEFAULT_CLEAR_REPAIRED = true;
    public static final boolean DEFAULT_VERIFY_SSTABLES = true;
    public static final boolean DEFAULT_VERIFY_TOKENS = true;
    public static final boolean DEFAULT_INVALIDATE_CACHES = true;
    public static final boolean DEFAULT_EXTENDED_VERIFY = true;
    public static final boolean DEFAULT_COPY_DATA = false;
    private final Vertx vertx;
    private final ExecutorPools executorPools;
    private final InstanceMetadataFetcher metadataFetcher;
    private final SSTableUploadsPathBuilder uploadPathBuilder;
    @VisibleForTesting
    final Map<String, ImportQueue> importQueuePerHost;

    /**
     * Constructs a new instance of the SSTableImporter class
     *
     * @param vertx             the vertx instance
     * @param metadataFetcher   a class for fetching InstanceMetadata
     * @param configuration     the configuration for Sidecar
     * @param executorPools     the executor pool
     * @param uploadPathBuilder a class that provides SSTableUploads directories
     */
    @Inject
    SSTableImporter(Vertx vertx,
                    InstanceMetadataFetcher metadataFetcher,
                    ServiceConfiguration configuration,
                    ExecutorPools executorPools,
                    SSTableUploadsPathBuilder uploadPathBuilder)
    {
        this.vertx = vertx;
        this.executorPools = executorPools;
        this.metadataFetcher = metadataFetcher;
        this.uploadPathBuilder = uploadPathBuilder;
        this.importQueuePerHost = new ConcurrentHashMap<>();
        executorPools.internal()
                     .setPeriodic(configuration.sstableImportConfiguration().importIntervalMillis(),
                                  this::processPendingImports);
    }

    /**
     * Queues an import with the provided import {@code options} to be processed asynchronously. The imports
     * are queued in a FIFO queue.
     *
     * @param options import options
     * @return a future for the result of the import
     */
    public Future<Void> scheduleImport(ImportOptions options)
    {
        Promise<Void> promise = Promise.promise();
        importQueuePerHost.computeIfAbsent(key(options), this::initializeQueue)
                          .offer(new AbstractMap.SimpleEntry<>(promise, options));
        return promise.future();
    }

    /**
     * Attempts to cancel an import for the provided {@code options}. This is a best-effort attempt, and
     * if the import has been started, it will not be cancelled.
     *
     * @param options import options
     * @return true if the options were removed from the queue, false otherwise
     */
    public boolean cancelImport(ImportOptions options)
    {
        ImportQueue queue = importQueuePerHost.get(key(options));
        boolean removed = false;
        if (queue != null)
        {
            removed = queue.removeIf(tuple -> options.equals(tuple.getValue()));
        }

        LOGGER.debug("Cancel import for options={} was {}removed", options, removed ? "" : "not ");
        return removed;
    }

    /**
     * Returns a key for the queues for the given {@code options}. Classes extending from {@link SSTableImporter}
     * can override the {@link #key(ImportOptions)} method and provide a different key for the queue.
     *
     * @param options the import options
     * @return a key for the queues for the given {@code options}
     */
    protected String key(ImportOptions options)
    {
        return options.host + "$" + options.keyspace + "$" + options.tableName;
    }

    /**
     * Returns a new queue for the given {@code key}. Classes extending from {@link SSTableImporter} can override
     * this method and provide a different implementation for the queue.
     *
     * @param key the key for the map
     * @return a new queue for the given {@code key}
     */
    protected ImportQueue initializeQueue(String key)
    {
        return new ImportQueue();
    }

    /**
     * Processes pending imports for every host in the import queue.
     *
     * @param timerId a unique identifier for the periodic timer
     */
    private void processPendingImports(Long timerId)
    {
        for (ImportQueue queue : importQueuePerHost.values())
        {
            if (!queue.isEmpty())
            {
                executorPools.internal()
                             .executeBlocking(p -> maybeDrainImportQueue(queue));
            }
        }
    }

    /**
     * Tries to lock the queue to perform the draining. If the queue is already being drained, then it will
     * not perform any operation.
     *
     * @param queue a queue of import tasks
     */
    private void maybeDrainImportQueue(ImportQueue queue)
    {
        if (queue.tryLock())
        {
            try
            {
                drainImportQueue(queue);
            }
            finally
            {
                queue.unlock();
            }
        }
    }

    /**
     * This blocking operation will drain the {@code queue}. It will utilize a single thread
     * to import the pending import requests on that host.
     *
     * @param queue a queue of import tasks
     */
    private void drainImportQueue(ImportQueue queue)
    {
        int successCount = 0, failureCount = 0;
        boolean recorded = false;
        InstanceMetrics instanceMetrics = null;
        while (!queue.isEmpty())
        {
            LOGGER.info("Starting SSTable import session");
            AbstractMap.SimpleEntry<Promise<Void>, ImportOptions> pair = queue.poll();
            Promise<Void> promise = pair.getKey();
            ImportOptions options = pair.getValue();

            InstanceMetadata instance = metadataFetcher.instance(options.host);
            CassandraAdapterDelegate delegate = instance.delegate();
            if (instanceMetrics == null)
            {
                instanceMetrics = instance.metrics();
            }

            if (delegate == null)
            {
                failureCount++;
                promise.fail(HttpExceptions.cassandraServiceUnavailable());
                continue;
            }

            if (!recorded)
            {
                // +1 offset added to consider already polled entry
                instance.metrics().sstableImport().pendingImports.metric.setValue(queue.size() + 1);
                recorded = true;
            }

            TableOperations tableOperations = delegate.tableOperations();
            if (tableOperations == null)
            {
                failureCount++;
                promise.fail(HttpExceptions.cassandraServiceUnavailable());
            }
            else
            {
                try
                {
                    long startTime = System.nanoTime();
                    List<String> failedDirectories =
                    tableOperations.importNewSSTables(options.keyspace,
                                                      options.tableName,
                                                      options.directory,
                                                      options.resetLevel,
                                                      options.clearRepaired,
                                                      options.verifySSTables,
                                                      options.verifyTokens,
                                                      options.invalidateCaches,
                                                      options.extendedVerify,
                                                      options.copyData);
                    long serviceTimeNanos = System.nanoTime() - startTime;
                    if (!failedDirectories.isEmpty())
                    {
                        failureCount++;
                        LOGGER.error("Failed to import SSTables with options={}, serviceTimeMillis={}, " +
                                     "failedDirectories={}", options, TimeUnit.NANOSECONDS.toMillis(serviceTimeNanos),
                                     failedDirectories);
                        promise.fail(new HttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                                       "Failed to import from directories: " + failedDirectories));
                    }
                    else
                    {
                        successCount++;
                        LOGGER.info("Successfully imported SSTables with options={}, serviceTimeMillis={}",
                                    options, TimeUnit.NANOSECONDS.toMillis(serviceTimeNanos));
                        promise.complete();
                        cleanup(options);
                    }
                }
                catch (Exception exception)
                {
                    failureCount++;
                    LOGGER.error("Failed to import SSTables with options={}", options, exception);
                    promise.fail(exception);
                }
            }
        }

        if (successCount > 0 || failureCount > 0)
        {
            LOGGER.info("Finished SSTable import session with successCount={}, failureCount={}",
                        successCount, failureCount);
            instanceMetrics.sstableImport().successfulImports.metric.update(successCount);
            instanceMetrics.sstableImport().failedImports.metric.update(failureCount);
        }
    }

    /**
     * Removes the staging directory recursively after a successful import
     *
     * @param options import options
     */
    private void cleanup(ImportOptions options)
    {
        uploadPathBuilder.resolveUploadIdDirectory(options.host, options.uploadId)
                         .compose(uploadPathBuilder::isValidDirectory)
                         .compose(stagingDirectory -> vertx.fileSystem()
                                                           .deleteRecursive(stagingDirectory, true))
                         .onSuccess(v ->
                                    LOGGER.debug("Successfully removed staging directory for uploadId={}, " +
                                                 "instance={}, options={}", options.uploadId, options.host, options))
                         .onFailure(cause ->
                                    LOGGER.error("Failed to remove staging directory for uploadId={}, " +
                                                 "instance={}, options={}", options.uploadId, options.host, options,
                                                 cause));
    }

    /**
     * A {@link ConcurrentLinkedQueue} that allows for locking the queue while operating on it. The queue
     * must be unlocked once the operations are complete.
     */
    static class ImportQueue extends ConcurrentLinkedQueue<AbstractMap.SimpleEntry<Promise<Void>, ImportOptions>>
    {
        private final AtomicBoolean isQueueInUse = new AtomicBoolean(false);

        /**
         * @return true if the queue was successfully locked, false otherwise
         */
        public boolean tryLock()
        {
            return isQueueInUse.compareAndSet(false, true);
        }

        /**
         * Unlocks the queue
         */
        public void unlock()
        {
            isQueueInUse.set(false);
        }
    }

    /**
     * Holds import options for importing SSTables into Cassandra
     */
    public static class ImportOptions
    {
        @NotNull
        final String host;
        @NotNull
        final String keyspace;
        @NotNull
        final String tableName;
        @NotNull
        final String directory;
        @NotNull
        final String uploadId;
        final boolean resetLevel;
        final boolean clearRepaired;
        final boolean verifySSTables;
        final boolean verifyTokens;
        final boolean invalidateCaches;
        final boolean extendedVerify;
        final boolean copyData;

        private ImportOptions(Builder builder)
        {
            host = Objects.requireNonNull(builder.host, "host is required");
            keyspace = Objects.requireNonNull(builder.keyspace, "keyspace is required");
            tableName = Objects.requireNonNull(builder.tableName, "tableName is required");
            directory = Objects.requireNonNull(builder.directory, "directory is required");
            uploadId = Objects.requireNonNull(builder.uploadId, "uploadId is required");
            resetLevel = builder.resetLevel;
            clearRepaired = builder.clearRepaired;
            verifySSTables = builder.verifySSTables;
            verifyTokens = builder.verifyTokens;
            invalidateCaches = builder.invalidateCaches;
            extendedVerify = builder.extendedVerify;
            copyData = builder.copyData;
        }

        /**
         * @return the host where the import takes place
         */
        public String host()
        {
            return host;
        }

        /**
         * @return the directory where the SSTable files are staged
         */
        public String directory()
        {
            return directory;
        }

        /**
         * @return the unique identifier for the upload
         */
        public String uploadId()
        {
            return uploadId;
        }

        /**
         * {@inheritDoc}
         */
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ImportOptions options = (ImportOptions) o;
            return resetLevel == options.resetLevel
                   && clearRepaired == options.clearRepaired
                   && verifySSTables == options.verifySSTables
                   && verifyTokens == options.verifyTokens
                   && invalidateCaches == options.invalidateCaches
                   && extendedVerify == options.extendedVerify
                   && copyData == options.copyData
                   && host.equals(options.host)
                   && keyspace.equals(options.keyspace)
                   && tableName.equals(options.tableName)
                   && directory.equals(options.directory)
                   && uploadId.equals(options.uploadId);
        }

        /**
         * {@inheritDoc}
         */
        public int hashCode()
        {
            return Objects.hash(host, keyspace, tableName, directory, uploadId, resetLevel, clearRepaired,
                                verifySSTables, verifyTokens, invalidateCaches, extendedVerify, copyData);
        }

        /**
         * {@inheritDoc}
         */
        public String toString()
        {
            return "ImportOptions{" +
                   "host='" + host + '\'' +
                   ", keyspace='" + keyspace + '\'' +
                   ", tableName='" + tableName + '\'' +
                   ", directory='" + directory + '\'' +
                   ", uploadId='" + uploadId + '\'' +
                   ", resetLevel=" + resetLevel +
                   ", clearRepaired=" + clearRepaired +
                   ", verifySSTables=" + verifySSTables +
                   ", verifyTokens=" + verifyTokens +
                   ", invalidateCaches=" + invalidateCaches +
                   ", extendedVerify=" + extendedVerify +
                   ", copyData=" + copyData +
                   '}';
        }

        /**
         * {@code ImportOptions} builder static inner class.
         */
        public static final class Builder
        {
            private String host;
            private String keyspace;
            private String tableName;
            private String directory;
            private String uploadId;
            private boolean resetLevel = DEFAULT_RESET_LEVEL;
            private boolean clearRepaired = DEFAULT_CLEAR_REPAIRED;
            private boolean verifySSTables = DEFAULT_VERIFY_SSTABLES;
            private boolean verifyTokens = DEFAULT_VERIFY_TOKENS;
            private boolean invalidateCaches = DEFAULT_INVALIDATE_CACHES;
            private boolean extendedVerify = DEFAULT_EXTENDED_VERIFY;
            private boolean copyData = DEFAULT_COPY_DATA;

            /**
             * Sets the {@code host} and returns a reference to this Builder enabling method chaining.
             *
             * @param host the {@code host} to set
             * @return a reference to this Builder
             */
            public Builder host(@NotNull String host)
            {
                this.host = host;
                return this;
            }

            /**
             * Sets the {@code keyspace} and returns a reference to this Builder enabling method chaining.
             *
             * @param keyspace the {@code keyspace} to set
             * @return a reference to this Builder
             */
            public Builder keyspace(@NotNull String keyspace)
            {
                this.keyspace = keyspace;
                return this;
            }

            /**
             * Sets the {@code tableName} and returns a reference to this Builder enabling method chaining.
             *
             * @param tableName the {@code tableName} to set
             * @return a reference to this Builder
             */
            public Builder tableName(@NotNull String tableName)
            {
                this.tableName = tableName;
                return this;
            }

            /**
             * Sets the {@code directory} and returns a reference to this Builder enabling method chaining.
             *
             * @param directory the {@code directory} to set
             * @return a reference to this Builder
             */
            public Builder directory(@NotNull String directory)
            {
                this.directory = directory;
                return this;
            }

            /**
             * Sets the {@code uploadId} and returns a reference to this Builder enabling method chaining.
             *
             * @param uploadId the {@code uploadId} to set
             * @return a reference to this Builder
             */
            public Builder uploadId(@NotNull String uploadId)
            {
                this.uploadId = uploadId;
                return this;
            }

            /**
             * Sets the {@code resetLevel} and returns a reference to this Builder enabling method chaining.
             *
             * @param resetLevel the {@code resetLevel} to set
             * @return a reference to this Builder
             */
            public Builder resetLevel(boolean resetLevel)
            {
                this.resetLevel = resetLevel;
                return this;
            }

            /**
             * Sets the {@code clearRepaired} and returns a reference to this Builder enabling method chaining.
             *
             * @param clearRepaired the {@code clearRepaired} to set
             * @return a reference to this Builder
             */
            public Builder clearRepaired(boolean clearRepaired)
            {
                this.clearRepaired = clearRepaired;
                return this;
            }

            /**
             * Sets the {@code verifySSTables} and returns a reference to this Builder enabling method chaining.
             *
             * @param verifySSTables the {@code verifySSTables} to set
             * @return a reference to this Builder
             */
            public Builder verifySSTables(boolean verifySSTables)
            {
                this.verifySSTables = verifySSTables;
                return this;
            }

            /**
             * Sets the {@code verifyTokens} and returns a reference to this Builder enabling method chaining.
             *
             * @param verifyTokens the {@code verifyTokens} to set
             * @return a reference to this Builder
             */
            public Builder verifyTokens(boolean verifyTokens)
            {
                this.verifyTokens = verifyTokens;
                return this;
            }

            /**
             * Sets the {@code invalidateCaches} and returns a reference to this Builder enabling method chaining.
             *
             * @param invalidateCaches the {@code invalidateCaches} to set
             * @return a reference to this Builder
             */
            public Builder invalidateCaches(boolean invalidateCaches)
            {
                this.invalidateCaches = invalidateCaches;
                return this;
            }

            /**
             * Sets the {@code extendedVerify} and returns a reference to this Builder enabling method chaining.
             *
             * @param extendedVerify the {@code extendedVerify} to set
             * @return a reference to this Builder
             */
            public Builder extendedVerify(boolean extendedVerify)
            {
                this.extendedVerify = extendedVerify;
                return this;
            }

            /**
             * Sets the {@code copyData} and returns a reference to this Builder enabling method chaining.
             *
             * @param copyData the {@code copyData} to set
             * @return a reference to this Builder
             */
            public Builder copyData(boolean copyData)
            {
                this.copyData = copyData;
                return this;
            }

            /**
             * Returns a {@code ImportOptions} built from the parameters previously set.
             *
             * @return a {@code ImportOptions} built with parameters of this {@code ImportOptions.Builder}
             */
            public ImportOptions build()
            {
                return new ImportOptions(this);
            }
        }
    }
}
