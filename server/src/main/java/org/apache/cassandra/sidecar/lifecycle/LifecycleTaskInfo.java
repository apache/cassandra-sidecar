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

package org.apache.cassandra.sidecar.lifecycle;

import org.apache.cassandra.sidecar.common.data.LifecycleCassandraState;
import org.apache.cassandra.sidecar.common.data.LifecycleStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a lifecycle task
 */
public class LifecycleTaskInfo
{
    private final LifecycleCassandraState intent;
    private final LifecycleStatus result;
    private final String message;

    public LifecycleTaskInfo(@NotNull LifecycleCassandraState intent, @NotNull LifecycleStatus result, @Nullable String message)
    {
        this.intent = intent;
        this.result = result;
        this.message = message;
    }

    public LifecycleCassandraState getIntent()
    {
        return intent;
    }

    @NotNull
    public LifecycleStatus getResult()
    {
        return result;
    }

    @Nullable
    public String getMessage()
    {
        return message;
    }
}
