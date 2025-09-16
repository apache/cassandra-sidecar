/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.cdc;

/**
 * Interface for capturing and reporting CDC (Change Data Capture) related statistics and metrics
 * in the Cassandra Sidecar application.
 */
public interface SidecarCdcStats
{
    /**
     * Cdc is enabled
     */
    default void captureCdcEnabled()
    {
    }

    /**
     * Cdc disabled and CdcPublisher will not be started.
     */
    default void captureCdcDisabled()
    {
    }

    // cdc lifecycle stats

    /**
     * CdcPublisher started successfully.
     *
     * @param numConsumers number of Cdc consumers started
     */
    default void captureCdcStarted(int numConsumers)
    {
    }

    /**
     * CdcPublisher failed to start.
     *
     * @param t throwable
     */
    default void captureCdcStartFailure(Throwable t)
    {
    }

    /**
     * CdcConsumer started.
     */
    default void captureCdcConsumerStarted()
    {
    }

    /**
     * CdcConsumer stopped.
     */
    default void captureCdcConsumerStopped()
    {
    }

    /**
     * Cdc or Kafka config change resulting in Cdc restart.
     */
    default void captureCdcConfigChange()
    {
    }

    /**
     * Cassandra cluster topology changed resulting in Cdc restart.
     */
    default void captureCdcClusterTopologyChange()
    {
    }

    /**
     * Cdc gained ownership for a token range.
     */
    default void captureCdcTokenRangeGained()
    {
    }

    /**
     * Cdc lost ownership for a token range.
     */
    default void captureCdcTokenRangeLost()
    {
    }

    /**
     * CdcPublisher restarted.
     */
    default void captureCdcRestart()
    {
    }

    /**
     * CdcPublisher stopped.
     */
    default void captureCdcStopped()
    {
    }

    /**
     * CdcPublisher failed to stop gracefully.
     *
     * @param t throwable
     */
    default void captureCdcStopFailed(Throwable t)
    {
    }

    /**
     * CdcConsumer read from state.
     *
     * @param count number of state objects.
     * @param len   byte length of state.
     */
    default void captureCdcConsumerReadFromState(int count, int len)
    {
    }

    default void captureCdcStateDeserializationFailure()
    {
    }

    /**
     * New blank CdcConsumer initialized.
     */
    default void captureNewBlankCdcConsumer()
    {
    }

    // cdc consumer stats

    /**
     * Cdc consumer completed
     *
     * @param epoch            epoch number, monotonically increasing 64-bit signed integer.
     * @param numDownInstances number of down instances
     * @param numTables        number of cached CDC enabled tables
     * @param runtimeNanos     runtime of the previous epoch in nanoseconds
     */
    default void captureCdcNextEpoch(long epoch, int numDownInstances, int numTables, long runtimeNanos)
    {

    }

    /**
     * Single Cdc event processed.
     */
    default void captureCdcEventProcessed()
    {
    }

    /**
     * Recoverable Error detected in the CdcConsumer.
     */
    default void captureRecoverableCdcError(Throwable t)
    {
    }

    /**
     * Unrecoverable Error detected in the CdcConsumer causing it to stop.
     */
    default void captureUnrecoverableCdcError(Throwable t)
    {
    }

    // state persist stats

    /**
     * Kafka queued events flushed.
     *
     * @param timeNanos time taken to flush in nanoseconds.
     */
    default void captureKafkaFlushTime(long timeNanos)
    {
    }

    /**
     * State persister backed up and not keeping up with persist cadence.
     *
     * @param numTasks number of active flush tasks queued.
     */
    default void capturePersistBackedUp(int numTasks)
    {
    }

    /**
     * Persist state request to storage.
     *
     * @param len byte length of state
     */
    default void capturePersistingCdcStateLength(int len)
    {
    }

    /**
     * Persist request succeeded.
     *
     * @param timeNanos time in nanos taken to persist.
     */
    default void capturePersistSucceeded(long timeNanos)
    {
    }

    /**
     * Persist failed with error.
     *
     * @param t throwable
     */
    default void capturePersistFailed(Throwable t)
    {
    }

    /**
     * Schema has been published.
     */
    default void capturePublishedSchema()
    {
    }

    /**
     * Cdc state has been written through http api.
     *
     * @param len length of the request body
     */
    default void captureCdcStatePutRequest(int len)
    {
    }

    /**
     * PutCdcStateHandler failed to write cdc state to store.
     */
    default void captureCdcStatePutRequestFailure()
    {
    }

    /**
     * Cdc state has been read through the http api.
     *
     * @param count number of Cdc state objects returned
     * @param len   length of the response body
     */
    default void captureCdcStateGetRequest(int count, int len)
    {
    }

    /**
     * A Cdc enabled table is not enabled in the Schema.instance singleton, this is a critical alert as the table will skipped when reading the commit log.
     */
    default void captureCdcTableNotEnabled()
    {
    }

    /**
     * cdc_on_repair is enabled in the yaml file and should be disabled.
     */
    default void captureCdcOnRepairEnabled()
    {
    }
}
