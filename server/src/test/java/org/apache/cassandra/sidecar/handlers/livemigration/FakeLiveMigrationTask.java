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

package org.apache.cassandra.sidecar.handlers.livemigration;


import org.apache.cassandra.sidecar.common.response.LiveMigrationTaskResponse;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationTask;

/**
 * Test implementation of LiveMigrationTask that returns predefined responses for testing purposes.
 */
public class FakeLiveMigrationTask implements LiveMigrationTask
{
    private final LiveMigrationTaskResponse taskResponse;
    private boolean cancelled = false;

    public FakeLiveMigrationTask(LiveMigrationTaskResponse taskResponse)
    {
        this.taskResponse = taskResponse;
    }

    @Override
    public String id()
    {
        return taskResponse.taskId();
    }

    @Override
    public void start()
    {
    }

    @Override
    public LiveMigrationTaskResponse getResponse()
    {
        return taskResponse;
    }

    @Override
    public void cancel()
    {
        cancelled = true;
    }

    @Override
    public boolean isCompleted()
    {
        if (cancelled)
            return true;

        String state = taskResponse.status().get(taskResponse.status().size() - 1).state();
        return "SUCCESS".equals(state) || "CANCELLED".equals(state) || "FAILED".equals(state);
    }
}
