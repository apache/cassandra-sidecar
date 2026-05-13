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

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.utils.UUIDs;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.common.server.exceptions.OperationalJobException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the {@link OperationalJobInfo} interface contract on {@link OperationalJob}
 */
class OperationalJobInfoTest
{
    @Test
    void testFailureReasonReturnsMessageForFailedJob()
    {
        String expectedMessage = "something went wrong";
        OperationalJob job = createFailingJob(expectedMessage);
        Promise<Void> promise = Promise.promise();
        job.execute(promise);

        assertThat(job.failureReason()).isEqualTo(expectedMessage);
    }

    @Test
    void testFailureReasonReturnsNullForSucceededJob()
    {
        OperationalJob job = createSucceedingJob();
        Promise<Void> promise = Promise.promise();
        job.execute(promise);

        assertThat(job.asyncResult().succeeded()).isTrue();
        assertThat(job.failureReason()).isNull();
    }

    @Test
    void testFailureReasonReturnsNullForCreatedJob()
    {
        OperationalJob job = createSucceedingJob();

        assertThat(job.asyncResult().isComplete()).isFalse();
        assertThat(job.failureReason()).isNull();
    }

    @Test
    void testNameReturnsFullNameForAnonymousClass()
    {
        OperationalJob job = createSucceedingJob();

        assertThat(job.name()).isNotEmpty();
        assertThat(job.name()).contains("OperationalJobInfoTest");
    }

    @Test
    void testNameReturnsSimpleNameForNamedClass()
    {
        OperationalJob job = new NamedJob(UUIDs.timeBased());

        assertThat(job.name()).isEqualTo("NamedJob");
    }

    static class NamedJob extends OperationalJob
    {
        NamedJob(UUID jobId)
        {
            super(jobId);
        }

        @Override
        public boolean hasConflict(List<OperationalJob> sameOperationJobs)
        {
            return false;
        }

        @Override
        protected Future<Void> executeInternal()
        {
            return Future.succeededFuture();
        }
    }

    private static OperationalJob createFailingJob(String failureMessage)
    {
        return new OperationalJob(UUIDs.timeBased())
        {
            @Override
            public boolean hasConflict(List<OperationalJob> sameOperationJobs)
            {
                return false;
            }

            @Override
            protected Future<Void> executeInternal() throws OperationalJobException
            {
                throw new OperationalJobException(failureMessage);
            }

        };
    }

    private static OperationalJob createSucceedingJob()
    {
        return new OperationalJob(UUIDs.timeBased())
        {
            @Override
            public boolean hasConflict(List<OperationalJob> sameOperationJobs)
            {
                return false;
            }

            @Override
            protected Future<Void> executeInternal()
            {
                return Future.succeededFuture();
            }
        };
    }
}
