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

package org.apache.cassandra.sidecar.tasks;

import java.util.concurrent.TimeUnit;

import io.vertx.core.Promise;

/**
 * An interface that defines a periodic task that will be executed during the lifecycle of Cassandra Sidecar
 */
public interface PeriodicTask extends Task<Void>
{
    /**
     * @return delay in the specified {@link #delayUnit()} for periodic task
     */
    long delay();

    /**
     * @return the unit for the {@link #delay()}, if not specified defaults to milliseconds
     */
    default TimeUnit delayUnit()
    {
        return TimeUnit.MILLISECONDS;
    }

    default long delayMillis()
    {
        return delayUnit().toMillis(delay());
    }

    /**
     * @return the initial delay for the task, defaults to the {@link #delay()}
     */
    default long initialDelay()
    {
        return delay();
    }

    /**
     * @return the units for the {@link #initialDelay()}, if not specified defaults to {@link #delayUnit()}
     */
    default TimeUnit initialDelayUnit()
    {
        return delayUnit();
    }

    default long initialDelayMillis()
    {
        return initialDelayUnit().toMillis(initialDelay());
    }

    /**
     * Register the periodic task executor at the task. By default, it is no-op.
     * If the reference to the executor is needed, the concrete {@link PeriodicTask} can implement this method
     *
     * @param executor the executor that manages the task
     */
    default void registerPeriodicTaskExecutor(PeriodicTaskExecutor executor)
    {
    }

    /**
     * Specify the schedule decision of the upcoming run.
     * The method is evaluated before calling {@link #execute(Promise)}
     *
     * @return schedule decision. The default is to {@link ScheduleDecision#EXECUTE}.
     */
    default ScheduleDecision scheduleDecision()
    {
        return ScheduleDecision.EXECUTE;
    }

    @Override
    default Void result()
    {
        throw new UnsupportedOperationException("No result is expected from a Periodic task");
    }
}
