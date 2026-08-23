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

package org.apache.cassandra.sidecar.adapters.cassandra60;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Collections;
import java.util.Map;
import javax.management.InstanceNotFoundException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.adapters.base.jmx.StorageJmxOperations;
import org.apache.cassandra.sidecar.adapters.cassandra60.jmx.SnapshotManagerJmxOperations;
import org.apache.cassandra.sidecar.common.server.JmxClient;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.server.exceptions.NodeBootstrappingException;
import org.apache.cassandra.sidecar.common.server.exceptions.SnapshotAlreadyExistsException;

import static org.apache.cassandra.sidecar.adapters.base.jmx.StorageJmxOperations.STORAGE_SERVICE_OBJ_NAME;
import static org.apache.cassandra.sidecar.adapters.cassandra60.jmx.SnapshotManagerJmxOperations.SNAPSHOT_MANAGER_OBJ_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link Cassandra60StorageOperations}, which routes the snapshot operations to the
 * Snapshot Manager MBean that Cassandra 6.0 introduced.
 */
class Cassandra60StorageOperationsTest
{
    private static final String TAG = "my-snapshot";
    private static final String KEYSPACE = "ks";
    private static final String TABLE = "tbl";

    private JmxClient mockJmxClient;
    private SnapshotManagerJmxOperations mockSnapshotManager;
    private StorageJmxOperations mockStorageService;
    private Cassandra60StorageOperations storageOperations;

    @BeforeEach
    void setUp()
    {
        mockJmxClient = mock(JmxClient.class);
        mockSnapshotManager = mock(SnapshotManagerJmxOperations.class);
        mockStorageService = mock(StorageJmxOperations.class);
        when(mockJmxClient.proxy(SnapshotManagerJmxOperations.class, SNAPSHOT_MANAGER_OBJ_NAME))
        .thenReturn(mockSnapshotManager);
        when(mockJmxClient.proxy(StorageJmxOperations.class, STORAGE_SERVICE_OBJ_NAME))
        .thenReturn(mockStorageService);
        storageOperations = new Cassandra60StorageOperations(mockJmxClient, mock(DnsResolver.class));
    }

    private static RuntimeException mBeanAbsentFailure()
    {
        return new UndeclaredThrowableException(new InstanceNotFoundException(SNAPSHOT_MANAGER_OBJ_NAME));
    }

    @Test
    void testTakeSnapshotUsesTheSnapshotManagerMBean() throws IOException
    {
        Map<String, String> options = Collections.singletonMap("skipFlush", "true");
        storageOperations.takeSnapshot(TAG, KEYSPACE, TABLE, options);

        verify(mockJmxClient, times(1)).proxy(SnapshotManagerJmxOperations.class, SNAPSHOT_MANAGER_OBJ_NAME);
        verify(mockJmxClient, never()).proxy(StorageJmxOperations.class, STORAGE_SERVICE_OBJ_NAME);
        verify(mockSnapshotManager, times(1)).takeSnapshot(TAG, options, KEYSPACE + "." + TABLE);
    }

    @Test
    void testClearSnapshotUsesTheSnapshotManagerMBean() throws IOException
    {
        storageOperations.clearSnapshot(TAG, KEYSPACE, TABLE);

        verify(mockJmxClient, times(1)).proxy(SnapshotManagerJmxOperations.class, SNAPSHOT_MANAGER_OBJ_NAME);
        verify(mockJmxClient, never()).proxy(StorageJmxOperations.class, STORAGE_SERVICE_OBJ_NAME);
        // the Snapshot Manager options map filters by age only, so the table is not passed
        verify(mockSnapshotManager, times(1)).clearSnapshot(TAG, Collections.emptyMap(), KEYSPACE);
    }

    @Test
    void testTakeSnapshotTranslatesTheCassandra60AlreadyExistsMessage() throws IOException
    {
        // the message that the Cassandra 6.0 Snapshot Manager produces for a duplicate tag
        String message = "Exception occured while executing org.apache.cassandra.service.snapshot.TakeSnapshotTask: "
                         + "Snapshot " + TAG + " for " + KEYSPACE + "." + TABLE + " already exists.";
        doThrow(new IOException(message))
        .when(mockSnapshotManager).takeSnapshot(TAG, Collections.emptyMap(), KEYSPACE + "." + TABLE);

        assertThatThrownBy(() -> storageOperations.takeSnapshot(TAG, KEYSPACE, TABLE, null))
        .isInstanceOf(SnapshotAlreadyExistsException.class);
    }

    @Test
    void testTakeSnapshotTranslatesTheBootstrapMessage() throws IOException
    {
        doThrow(new IOException("Cannot snapshot until bootstrap completes"))
        .when(mockSnapshotManager).takeSnapshot(TAG, Collections.emptyMap(), KEYSPACE + "." + TABLE);

        assertThatThrownBy(() -> storageOperations.takeSnapshot(TAG, KEYSPACE, TABLE, null))
        .isInstanceOf(NodeBootstrappingException.class);
    }

    @Test
    void testTakeSnapshotReplacesNullOptionsWithAnEmptyMap() throws IOException
    {
        // SnapshotOptions.userSnapshot reads the map without a null check
        storageOperations.takeSnapshot(TAG, KEYSPACE, TABLE, null);

        verify(mockSnapshotManager, times(1)).takeSnapshot(TAG, Collections.emptyMap(), KEYSPACE + "." + TABLE);
    }

    @Test
    void testClearSnapshotTranslatesAnIOException() throws IOException
    {
        // the Snapshot Manager MBean declares IOException, although the 6.0 clear implementation raises a
        // RuntimeException instead
        doThrow(new IOException("Keyspace " + KEYSPACE + " does not exist"))
        .when(mockSnapshotManager).clearSnapshot(TAG, Collections.emptyMap(), KEYSPACE);

        assertThatThrownBy(() -> storageOperations.clearSnapshot(TAG, KEYSPACE, TABLE))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testClearSnapshotClassifiesAFailureCarriedOnlyByTheCause() throws IOException
    {
        // SnapshotManager wraps a clear failure without appending the task message, so the text is in the cause alone
        RuntimeException wrapped = new RuntimeException(
        "Exception occured while executing org.apache.cassandra.service.snapshot.ClearSnapshotTask",
        new RuntimeException("Keyspace " + KEYSPACE + " does not exist"));
        doThrow(wrapped).when(mockSnapshotManager).clearSnapshot(TAG, Collections.emptyMap(), KEYSPACE);

        assertThatThrownBy(() -> storageOperations.clearSnapshot(TAG, KEYSPACE, TABLE))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testClearSnapshotRethrowsAnUnrelatedRuntimeFailure() throws IOException
    {
        RuntimeException unrelated = new RuntimeException("the JMX connection is closed");
        doThrow(unrelated).when(mockSnapshotManager).clearSnapshot(TAG, Collections.emptyMap(), KEYSPACE);

        assertThatThrownBy(() -> storageOperations.clearSnapshot(TAG, KEYSPACE, TABLE))
        .isSameAs(unrelated);
    }

    @Test
    void testTakeSnapshotTranslatesTheWrappedCassandra60Message() throws IOException
    {
        // SnapshotManager.takeSnapshot wraps its failures as new IOException(cause), so the message is cause.toString()
        RuntimeException cause = new RuntimeException(
        "Exception occured while executing org.apache.cassandra.service.snapshot.TakeSnapshotTask: "
        + "Snapshot " + TAG + " for " + KEYSPACE + "." + TABLE + " already exists.");
        doThrow(new IOException(cause))
        .when(mockSnapshotManager).takeSnapshot(TAG, Collections.emptyMap(), KEYSPACE + "." + TABLE);

        assertThatThrownBy(() -> storageOperations.takeSnapshot(TAG, KEYSPACE, TABLE, null))
        .isInstanceOf(SnapshotAlreadyExistsException.class);
    }

    @Test
    void testTakeSnapshotFallsBackWhenTheMBeanIsAbsent() throws IOException
    {
        // only CassandraDaemon registers the Snapshot Manager MBean, so an in-JVM dtest node does not export it
        Map<String, String> options = Collections.singletonMap("skipFlush", "true");
        doThrow(mBeanAbsentFailure())
        .when(mockSnapshotManager).takeSnapshot(TAG, options, KEYSPACE + "." + TABLE);

        storageOperations.takeSnapshot(TAG, KEYSPACE, TABLE, options);

        verify(mockStorageService, times(1)).takeSnapshot(TAG, options, KEYSPACE + "." + TABLE);
    }

    @Test
    void testClearSnapshotFallsBackWhenTheMBeanIsAbsent() throws IOException
    {
        doThrow(mBeanAbsentFailure())
        .when(mockSnapshotManager).clearSnapshot(TAG, Collections.emptyMap(), KEYSPACE);

        storageOperations.clearSnapshot(TAG, KEYSPACE, TABLE);

        verify(mockStorageService, times(1)).clearSnapshot(TAG, KEYSPACE);
    }

    @Test
    void testTheFallbackIsRememberedAcrossCalls() throws IOException
    {
        doThrow(mBeanAbsentFailure())
        .when(mockSnapshotManager).clearSnapshot(TAG, Collections.emptyMap(), KEYSPACE);

        storageOperations.clearSnapshot(TAG, KEYSPACE, TABLE);
        storageOperations.clearSnapshot(TAG, KEYSPACE, TABLE);
        storageOperations.takeSnapshot(TAG, KEYSPACE, TABLE, null);

        // the second clear and the take go straight to the Storage Service MBean
        verify(mockSnapshotManager, times(1)).clearSnapshot(TAG, Collections.emptyMap(), KEYSPACE);
        verify(mockSnapshotManager, never()).takeSnapshot(TAG, Collections.emptyMap(), KEYSPACE + "." + TABLE);
        verify(mockStorageService, times(2)).clearSnapshot(TAG, KEYSPACE);
        verify(mockStorageService, times(1)).takeSnapshot(TAG, Collections.emptyMap(), KEYSPACE + "." + TABLE);
    }

    @Test
    void testTakeSnapshotRequiresNonNullArguments()
    {
        assertThatThrownBy(() -> storageOperations.takeSnapshot(null, KEYSPACE, TABLE, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("snapshot tag must be non-null");
        assertThatThrownBy(() -> storageOperations.clearSnapshot(TAG, null, TABLE))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("keyspace must be non-null");
    }

    @Test
    void testSnapshotManagerObjectNameMatchesCassandra60()
    {
        assertThat(SNAPSHOT_MANAGER_OBJ_NAME).isEqualTo("org.apache.cassandra.service.snapshot:type=SnapshotManager");
    }
}
