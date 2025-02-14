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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Closeable;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.TaskExecutorPool;
import org.apache.cassandra.sidecar.coordination.ClusterLease;
import org.apache.cassandra.sidecar.coordination.ExecuteOnClusterLeaseholderOnly;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * This class manages the scheduling and execution of {@link PeriodicTask}s.
 * For the {@link PeriodicTask} that also is {@link ExecuteOnClusterLeaseholderOnly}, the executor ensures
 * the task execution is only performed when {@link ClusterLease} is claimed by the local Sidecar instance.
 *
 * <p>The execution of each {@link PeriodicTask} is <i>ordered</i> and <i>serial</i>, meanwhile there could
 * be concurrent execution of different {@link PeriodicTask}s.
 * <p>Memory consistency effects: Actions in the prior {@link PeriodicTask} run <i>happen-before</i> its
 * next run, perhaps in another thread. In other words, writes in the prior run can be read by its next run,
 * as if it is running in a single thread.
 */
public class PeriodicTaskExecutor implements Closeable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicTaskExecutor.class);
    private static final long RUN_NOW_TIMER_ID = -1L; // used when the run start immediately, not scheduled via a timer
    private static final long UNSCHEDULED_STATE_TIMER_ID = -2L; // used when the task is unscheduled

    // keep track of the timerIds in order to cancel them when closing/unscheduling
    private final Map<PeriodicTaskKey, Long> timerIds = new ConcurrentHashMap<>();
    private final Map<PeriodicTaskKey, Future<Void>> activeRuns = new ConcurrentHashMap<>();
    private final TaskExecutorPool internalPool;
    private final ClusterLease clusterLease;

    public PeriodicTaskExecutor(ExecutorPools executorPools, ClusterLease clusterLease)
    {
        this.internalPool = executorPools.internal();
        this.clusterLease = clusterLease;
    }

    /**
     * Schedules the {@code task} iff it has not been scheduled yet.
     * <p>A task is identified by the combination of its class name and the {@link PeriodicTask#name()}, see {@link PeriodicTaskKey}.
     * There is one and exactly one task of the same identity can be scheduled.
     *
     * @param task the task to execute
     */
    public void schedule(PeriodicTask task)
    {
        PeriodicTaskKey key = new PeriodicTaskKey(task);
        schedule(key, 0, task.initialDelay().to(TimeUnit.MILLISECONDS), 0);
    }

    private void schedule(PeriodicTaskKey key, long priorExecDurationMillis, long delayMillis, long execCount)
    {
        long actualDelayMillis = delayMillis - priorExecDurationMillis;
        AtomicBoolean runNow = new AtomicBoolean(actualDelayMillis <= 0);
        timerIds.compute(key, (k, tid) -> {
            // The periodic task has been scheduled already. Exit early and avoid scheduling the duplication
            if (tid != null && execCount == 0)
            {
                LOGGER.debug("Task is already scheduled. task='{}'", key);
                runNow.set(false);
                return tid;
            }

            // Cleanup the unscheduled task from map
            if (tid != null && tid == UNSCHEDULED_STATE_TIMER_ID) // at this step, execCount != 0
            {
                LOGGER.debug("Task is now unscheduled. task='{}' execCount={}", key, execCount);
                runNow.set(false);
                return null; // remove the entry from the map, since it is unscheduled
            }

            if (tid == null && execCount != 0)
            {
                LOGGER.info("The executor is closed or the task is already unscheduled. " +
                            "Avoid scheduling more runs." +
                            "tid=null task='{}' execCount={}", key, execCount);
                runNow.set(false);
                return null;
            }

            LOGGER.debug("Scheduling task {}. task='{}' execCount={}",
                         runNow.get() ? "immediately" : "in " + actualDelayMillis + " milliseconds",
                         key, execCount);

            try
            {
                key.task.registerPeriodicTaskExecutor(this);
            }
            catch (Exception e)
            {
                LOGGER.warn("Failed to invoke registerPeriodicTaskExecutor. task='{}'", key, e);
            }

            // If run immediately, do not execute within the compute block.
            // Return the placeholder timer ID, and execute after exiting the compute block.
            if (runNow.get())
            {
                return RUN_NOW_TIMER_ID; // use the placeholder ID, since this run is not scheduled as a timer
            }
            // Schedule and update the timer id
            return internalPool.setTimer(delayMillis, timerId -> executeAndScheduleNext(key, execCount));
        });

        if (runNow.get())
        {
            executeAndScheduleNext(key, execCount);
        }
    }

    /**
     * This method ensures the happens-before memory consistency effect between runs of the same {@link PeriodicTask}.
     * <p>Each run, essentially, is executed at {@link java.util.concurrent.Executor#execute(Runnable)}. At the end of the
     * execution, the executor schedules a new run. When the scheduled time comes, the next run is executed.
     * There are the following happens-before relationships, <i>hb(prior_run, scheduler></i> and <i>hb(scheduler, next_run)</i>.
     * Therefore, <i>hb(prior_run, next_run)</i>, i.e. the effects from prior_run are visible to next_run.
     * <p>More on <a href="https://docs.oracle.com/javase/specs/jls/se11/html/jls-17.html#jls-17.4">Java Memory Model</a>
     */
    private void executeAndScheduleNext(PeriodicTaskKey key, long execCount)
    {
        Promise<Void> runPromise = Promise.promise();
        if (activeRuns.computeIfAbsent(key, k -> runPromise.future()) != runPromise)
        {
            LOGGER.debug("already active. task='{}' execCount={}", key, execCount);
            return;
        }
        long startTime = System.nanoTime();
        internalPool.<ScheduleDecision>executeBlocking(promise -> executeInternal(promise, key, execCount), false)
                    .onComplete(outcome -> {
                        LOGGER.debug("Task run finishes. task='{}' outcome={} execCount={}", key, outcome, execCount);
                        runPromise.complete(); // mark the completion, regardless of the result from last run
                        activeRuns.remove(key);

                        DurationSpec delay;
                        long priorExecutionDurationMillis;
                        if (outcome.result() == ScheduleDecision.RESCHEDULE)
                        {
                            priorExecutionDurationMillis = 0;
                            delay = key.task.initialDelay();
                        }
                        else
                        {
                            priorExecutionDurationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
                            delay = key.task.delay();
                        }

                        schedule(key, priorExecutionDurationMillis, delay.to(TimeUnit.MILLISECONDS), execCount + 1);
                    });
    }

    /**
     * Unschedule and close the {@link PeriodicTask} iff it has been scheduled.
     *
     * @param task the task to unschedule
     * @return future of task unscheduling
     */
    public Future<Void> unschedule(PeriodicTask task)
    {
        PeriodicTaskKey key = new PeriodicTaskKey(task);
        AtomicBoolean alreadyUnscheduled = new AtomicBoolean(false);
        Long timerId = timerIds.computeIfPresent(key, (k, tid) -> {
            alreadyUnscheduled.set(tid == UNSCHEDULED_STATE_TIMER_ID);
            if (tid > 0)
            {
                // Best-effort on cancelling the timer
                internalPool.cancelTimer(tid);
            }
            return UNSCHEDULED_STATE_TIMER_ID;
        });
        if (timerId == null)
        {
            LOGGER.debug("No such task to unschedule. task='{}'", key);
            return Future.failedFuture("No such task to unschedule");
        }

        if (alreadyUnscheduled.get())
        {
            LOGGER.debug("Task is already unscheduled. task='{}'", key);
            return Future.failedFuture("Task is already unscheduled");
        }

        LOGGER.debug("Unscheduling task. task='{}' timerId={}", key, timerId);

        // The current run might have started already.
        // If so, close the task once the run is completed; the task entry is removed on the next schedule.
        // Otherwise, the task entry should be removed here, as there are no more schedules.
        boolean removeEntry = !activeRuns.containsKey(key);
        return activeRuns
               .getOrDefault(key, Future.succeededFuture())
               .andThen(ignored -> {
                   try
                   {
                       task.close();
                   }
                   catch (Throwable cause)
                   {
                       // just log any error while closing and continue
                       LOGGER.warn("Failed to close task during unscheduling. task='{}'", key, cause);
                   }
                   if (removeEntry)
                   {
                       timerIds.remove(key);
                   }
               });
    }

    @Override
    public void close(Promise<Void> completion)
    {
        LOGGER.info("Closing...");
        try
        {
            timerIds.keySet().forEach(key -> unschedule(key.task));
            timerIds.clear();
            completion.complete();
        }
        catch (Throwable throwable)
        {
            completion.fail(throwable);
        }
    }

    private void executeInternal(Promise<ScheduleDecision> promise, PeriodicTaskKey key, long execCount)
    {
        PeriodicTask periodicTask = key.task;
        ScheduleDecision scheduleDecision = consolidateScheduleDecision(periodicTask);
        LOGGER.debug("{} task. task='{}' execCount={}", scheduleDecision, key, execCount);
        if (scheduleDecision == ScheduleDecision.EXECUTE)
        {
            Promise<Void> taskRunPromise = Promise.promise();
            taskRunPromise.future().onComplete(ignored -> promise.tryComplete(ScheduleDecision.EXECUTE));
            try
            {
                periodicTask.execute(taskRunPromise);
            }
            catch (Throwable throwable)
            {
                LOGGER.warn("Periodic task failed to execute. task='{}' execCount={}", periodicTask.name(), execCount, throwable);
                taskRunPromise.tryFail(throwable);
            }
        }
        else
        {
            promise.tryComplete(scheduleDecision);
        }
    }

    /**
     * Consolidate the {@link ScheduleDecision} from the {@link PeriodicTask} and {@link ClusterLease}
     *
     * @param periodicTask the task
     * @return schedule decision
     */
    protected ScheduleDecision consolidateScheduleDecision(PeriodicTask periodicTask)
    {
        ScheduleDecision decisionFromTask = periodicTask.scheduleDecision();
        // Take the cluster lease ownership into consideration, if the periodic task
        // decides to execute or reschedule.
        // For example, if the local sidecar is not the cluster leaseholder, the lease
        // ownership should override the decision.
        if (decisionFromTask != ScheduleDecision.SKIP
            && periodicTask instanceof ExecuteOnClusterLeaseholderOnly)
        {
            return clusterLease.toScheduleDecision();
        }
        return decisionFromTask;
    }

    // A simple wrapper that implements equals and hashcode,
    // which is not necessary for the actual ExecutionLoops to implement
    static class PeriodicTaskKey
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

        @Override
        public String toString()
        {
            return fqcnAndName;
        }
    }

    @VisibleForTesting
    Map<PeriodicTaskKey, Long> timerIds()
    {
        return timerIds;
    }

    @VisibleForTesting
    Map<PeriodicTaskKey, Future<Void>> activeRuns()
    {
        return activeRuns;
    }
}
