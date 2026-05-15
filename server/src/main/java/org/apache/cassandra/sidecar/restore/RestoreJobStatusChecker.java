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

package org.apache.cassandra.sidecar.restore;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.config.RestoreJobConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreJobDatabaseAccessor;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.apache.cassandra.sidecar.tasks.ScheduleDecision;

/**
 * Fast status-check loop complement to {@link RestoreJobDiscoverer}. Runs on a short interval
 * (see {@link RestoreJobConfiguration#jobDiscoveryStatusCheckInterval()}) and performs cheap
 * point-reads of {@code job.status} for the in-flight jobs already known to the discoverer.
 * When a status transition is detected it delegates to
 * {@link RestoreJobDiscoverer#handleStatusTransition(RestoreJob)} so that sidecars react to
 * phase signals without waiting for the slow full-scan loop. The slow loop remains the
 * correctness and recovery guarantee (new-job discovery, expired-job aborts, etc.).
 */
@Singleton
public class RestoreJobStatusChecker implements PeriodicTask
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RestoreJobStatusChecker.class);

    private final RestoreJobConfiguration config;
    private final SidecarSchema sidecarSchema;
    private final RestoreJobDatabaseAccessor restoreJobDatabaseAccessor;
    private final RestoreJobDiscoverer restoreJobDiscoverer;
    private final AtomicBoolean isExecuting = new AtomicBoolean(false);

    @Inject
    public RestoreJobStatusChecker(SidecarConfiguration sidecarConfig,
                                   SidecarSchema sidecarSchema,
                                   RestoreJobDatabaseAccessor restoreJobDatabaseAccessor,
                                   RestoreJobDiscoverer restoreJobDiscoverer)
    {
        this.config = sidecarConfig.restoreJobConfiguration();
        this.sidecarSchema = sidecarSchema;
        this.restoreJobDatabaseAccessor = restoreJobDatabaseAccessor;
        this.restoreJobDiscoverer = restoreJobDiscoverer;
    }

    @Override
    public DurationSpec delay()
    {
        return config.jobDiscoveryStatusCheckInterval();
    }

    @Override
    public ScheduleDecision scheduleDecision()
    {
        if (!sidecarSchema.isInitialized())
        {
            return ScheduleDecision.SKIP;
        }
        if (isExecuting.get())
        {
            return ScheduleDecision.SKIP;
        }
        if (restoreJobDiscoverer.inflightJobIds().isEmpty())
        {
            return ScheduleDecision.SKIP;
        }
        return ScheduleDecision.EXECUTE;
    }

    @Override
    public void execute(Promise<Void> promise)
    {
        if (!isExecuting.compareAndSet(false, true))
        {
            promise.tryComplete();
            return;
        }

        try
        {
            Set<UUID> jobIds = restoreJobDiscoverer.inflightJobIds();
            for (UUID jobId : jobIds)
            {
                try
                {
                    RestoreJob current = restoreJobDatabaseAccessor.find(jobId);
                    if (current == null)
                    {
                        continue;
                    }
                    restoreJobDiscoverer.handleStatusTransition(current);
                }
                catch (Exception e)
                {
                    // Do not fail the whole pass on one job; the slow loop will retry it.
                    LOGGER.warn("Exception on status check for jobId={}", jobId, e);
                }
            }
        }
        finally
        {
            isExecuting.set(false);
            promise.tryComplete();
        }
    }
}
