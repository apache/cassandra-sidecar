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

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.sidecar.common.data.OperationType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An {@link OperationalJobCoordinator} used when a Sidecar instance is not configured to support
 * coordinated cluster-wide operations.
 * <p>
 * Jobs that do not require coordination ({@link OperationalJob#requiresCoordination()} returns
 * {@code false}) never reach this coordinator, so uncoordinated operations (e.g. decommission) are
 * unaffected. 
 */
public class DisabledOperationalJobCoordinator implements OperationalJobCoordinator
{
    private static final String NOT_SUPPORTED_MESSAGE =
    "Operational job coordination is not supported by this Sidecar instance. "
    + "Configure a coordinator to enable coordinated cluster-wide operations.";

    @Override
    public boolean trySetActive(OperationType operationType, UUID operationId)
    {
        throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }

    @Override
    public boolean clearActive(OperationType operationType, UUID operationId)
    {
        throw new UnsupportedOperationException(NOT_SUPPORTED_MESSAGE);
    }

    @Override
    @Nullable
    public UUID getActiveOperation(OperationType operationType)
    {
        return null;
    }

    @Override
    @NotNull
    public Map<OperationType, UUID> getActiveOperations()
    {
        return Collections.emptyMap();
    }
}
