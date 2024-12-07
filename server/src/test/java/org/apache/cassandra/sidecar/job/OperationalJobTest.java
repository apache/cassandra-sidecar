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

import java.util.UUID;
import org.junit.jupiter.api.Test;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.common.server.exceptions.OperationalJobException;
import org.apache.cassandra.sidecar.common.utils.OperationalJobResult;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests to validate the Job APIs
 */
public class OperationalJobTest
{
    private static final Vertx vertx = Vertx.vertx();
    public static OperationalJob createJobWithStatus(OperationalJobResult.OperationalJobStatus jobStatus)
    {
        return new OperationalJob(vertx, UUID.randomUUID(), jobStatus)
        {
            @Override
            protected OperationalJobResult.OperationalJobStatus executeInternal()
            {
                return OperationalJobResult.OperationalJobStatus.COMPLETED;
            }
            public String operation()
            {
                return "test";
            }

            public boolean isRunningDownstream()
            {
                return false;
            }
        };
    }

    @Test
    void testJobCompletion()
    {
        OperationalJob job = createJobWithStatus(OperationalJobResult.OperationalJobStatus.COMPLETED);
        Promise p = Promise.promise();
        job.execute(p);
        Future<OperationalJobResult.OperationalJobStatus> statusFuture = p.future();
        assertThat(statusFuture.isComplete()).isTrue();
        assertThat(statusFuture.result()).isEqualTo(OperationalJobResult.OperationalJobStatus.COMPLETED);
    }

    @Test
    void testJobFailed()
    {
        String msg = "Test Job failed";
        OperationalJob failingJob = new OperationalJob(vertx, UUID.randomUUID())
        {
            protected OperationalJobResult.OperationalJobStatus executeInternal() throws OperationalJobException
            {
                throw new OperationalJobException(msg);
            }

            public String operation()
            {
                return "test";
            }

            public boolean isRunningDownstream()
            {
                return false;
            }
        };

        Promise p = Promise.promise();
        failingJob.execute(p);

        Future<OperationalJobResult.OperationalJobStatus> statusFuture = p.future();
        assertThat(statusFuture.failed()).isTrue();
        assertThat(statusFuture.cause().getClass()).isEqualTo(OperationalJobException.class);
        assertThat(statusFuture.cause().getMessage()).isEqualTo(msg);
    }
}
