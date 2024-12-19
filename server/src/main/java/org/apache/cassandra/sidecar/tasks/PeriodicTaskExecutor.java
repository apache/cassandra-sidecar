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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Closeable;
import io.vertx.core.Promise;
import io.vertx.core.impl.ConcurrentHashSet;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.coordination.ClusterLease;
import org.apache.cassandra.sidecar.coordination.ExecuteOnClusterLeaseholderOnly;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * This class manages the scheduling and execution of {@link PeriodicTask}s.
 */
@Singleton
public class PeriodicTaskExecutor implements Closeable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicTaskExecutor.class);

    private final Map<PeriodicTaskKey, Long> timerIds = new ConcurrentHashMap<>();
    private final Set<PeriodicTaskKey> activeTasks = new ConcurrentHashSet<>();
    private final TaskExecutorPool internalPool;
    private final ClusterLease clusterLease;

    @VisibleForTesting
    public PeriodicTaskExecutor(ExecutorPools executorPools)
    {
        this(executorPools, null);
    }

    @Inject
    public PeriodicTaskExecutor(ExecutorPools executorPools, ClusterLease clusterLease)
    {
        this.internalPool = executorPools.internal();
        this.clusterLease = clusterLease;
    }

    /**
     * Schedules the {@code task} iff it has not been scheduled yet.
     *
     * @param task the task to execute
     * @return the identifier of the timer associated with this task
     */
    public long schedule(PeriodicTask task)
    {
        PeriodicTaskKey key = new PeriodicTaskKey(task);
        return timerIds.computeIfAbsent(key, k -> {
            task.registerPeriodicTaskExecutor(this);
            long initialDelayMillis = task.initialDelayUnit().toMillis(task.initialDelay());
            long delayMillis = task.delayUnit().toMillis(task.delay());
            return internalPool.setPeriodic(initialDelayMillis, delayMillis, id -> executeInternal(k));
        });
    }

    /**
     * Unschedules the {@code task} iff the task has been registered previously and returns the identifier
     * of the timer associated with this task, or {@code -1} if the task is not registered.
     *
     * @param task the task to unschedule
     * @return the identifier of the timer associated with this task, or {@code -1} if the task is not registered
     */
    public long unschedule(PeriodicTask task)
    {
        Long timerId = timerIds.remove(new PeriodicTaskKey(task));
        if (timerId != null)
        {
            internalPool.cancelTimer(timerId);
            task.close();
            return timerId;
        }
        return -1L; // valid timer ids are non-negative
    }

    /**
     * Reschedules the provided {@code task} and returns the identifier of the timer associated with the rescheduled
     * task.
     *
     * @param task the task to reschedule
     * @return the identifier of the timer associated with the rescheduled task
     */
    public long reschedule(PeriodicTask task)
    {
        Long timerId = timerIds.remove(new PeriodicTaskKey(task));
        if (timerId != null)
        {
            internalPool.cancelTimer(timerId);
        }
        return schedule(task);
    }

    @Override
    public void close(Promise<Void> completion)
    {
        try
        {
            timerIds.values().forEach(internalPool::cancelTimer);
            timerIds.keySet().forEach(key -> key.task.close());
            timerIds.clear();
            activeTasks.clear();
            completion.complete();
        }
        catch (Throwable throwable)
        {
            completion.fail(throwable);
        }
    }

    private void executeInternal(PeriodicTaskKey key)
    {
        PeriodicTask periodicTask = key.task;

        switch (determineExecution(periodicTask))
        {
            case SKIP_EXECUTION:
                LOGGER.trace("Skip executing task. task={}", periodicTask.name());
                return;

            case EXECUTE:
                break;

            case INDETERMINATE:
            default:
                LOGGER.debug("Unable to determine execution for this task, rescheduling. task={}", periodicTask.name());
                // When the process is unable to determine whether a task should be executed, we want
                // the task to be rescheduled for a shorter period of time. Assume you have a PeriodicTask that
                // runs every day. If it happens to run during a period of time when there's no
                // determination whether task should run or not, we do not want to wait another day for the
                // task to run, so instead we reschedule it and only wait the initial delay of the task for
                // the task to try again.
                reschedule(periodicTask);
                return;
        }

        if (!activeTasks.add(key))
        {
            LOGGER.debug("Task is already running. task={}", periodicTask.name());
            return;
        }

        Promise<Void> promise = Promise.promise();

        try
        {
            periodicTask.execute(promise);
        }
        catch (Throwable throwable)
        {
            LOGGER.warn("Periodic task failed to execute. task={}", periodicTask.name(), throwable);
            promise.tryFail(throwable);
        }

        promise.future().onComplete(res -> activeTasks.remove(key));
    }

    /**
     * Determines whether the task should run.
     *
     * @param periodicTask the task
     * @return the result of the determination
     */
    protected ExecutionDetermination determineExecution(PeriodicTask periodicTask)
    {
        if (periodicTask.shouldSkip())
        {
            return ExecutionDetermination.SKIP_EXECUTION;
        }

        if (periodicTask instanceof ExecuteOnClusterLeaseholderOnly)
        {
            return clusterLease.executionDetermination();
        }
        return ExecutionDetermination.EXECUTE;
    }

    // A simple wrapper that implements equals and hashcode,
    // which is not necessary for the actual ExecutionLoops to implement
    private static class PeriodicTaskKey
    {
        private final String fqcnAndName;
        private final PeriodicTask task;

        PeriodicTaskKey(PeriodicTask task)
        {
            this.fqcnAndName = task.getClass().getCanonicalName() + task.name();
            this.task = task;
        }

        @Override
        public int hashCode()
        {
            return fqcnAndName.hashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;

            if (obj instanceof PeriodicTaskKey)
            {
                return ((PeriodicTaskKey) obj).fqcnAndName.equals(this.fqcnAndName);
            }

            return false;
        }
    }
}
