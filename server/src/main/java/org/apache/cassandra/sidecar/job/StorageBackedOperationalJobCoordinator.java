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

package org.apache.cassandra.sidecar.job;

import java.util.Map;
import java.util.UUID;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.common.data.OperationType;
import org.apache.cassandra.sidecar.job.storage.StorageProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An {@link OperationalJobCoordinator} implementation that delegates to a {@link StorageProvider}
 * for coordination of active operations.
 */
@Singleton
public class StorageBackedOperationalJobCoordinator implements OperationalJobCoordinator
{
    private final StorageProvider storageProvider;

    @Inject
    public StorageBackedOperationalJobCoordinator(StorageProvider storageProvider)
    {
        this.storageProvider = storageProvider;
    }

    @Override
    public boolean trySetActive(OperationType operationType, UUID operationId)
    {
        return storageProvider.trySetActiveOperation(operationType, operationId);
    }

    @Override
    public boolean clearActive(OperationType operationType, UUID operationId)
    {
        return storageProvider.clearActiveOperation(operationType, operationId);
    }

    @Override
    @Nullable
    public UUID getActiveOperation(OperationType operationType)
    {
        return storageProvider.getActiveOperation(operationType);
    }

    @Override
    @NotNull
    public Map<OperationType, UUID> getActiveOperations()
    {
        return storageProvider.getActiveOperations();
    }
}
