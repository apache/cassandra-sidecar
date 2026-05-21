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

package org.apache.cassandra.sidecar.livemigration;

import org.apache.cassandra.sidecar.common.response.LiveMigrationFilesVerificationResponse;
import org.apache.cassandra.sidecar.livemigration.LiveMigrationFilesVerificationTask.State;

import static org.apache.cassandra.sidecar.livemigration.LiveMigrationFilesVerificationTask.FILES_VERIFICATION_TASK_TYPE;

/**
 * Fake implementation of LiveMigrationFilesVerificationTask for testing purposes.
 */
public class FakeFilesVerificationTask implements LiveMigrationTask<LiveMigrationFilesVerificationResponse>
{
    private final LiveMigrationFilesVerificationResponse response;
    private boolean cancelled = false;

    public FakeFilesVerificationTask(LiveMigrationFilesVerificationResponse response)
    {
        this.response = response;
    }

    @Override
    public String id()
    {
        return response.id();
    }

    @Override
    public String type()
    {
        return FILES_VERIFICATION_TASK_TYPE;
    }

    @Override
    public void start()
    {
        // No-op for fake implementation - response is pre-configured
    }

    @Override
    public LiveMigrationFilesVerificationResponse getResponse()
    {
        return response;
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

        State state = State.valueOf(response.state());
        return state == State.COMPLETED || state == State.CANCELLED || state == State.FAILED;
    }
}
