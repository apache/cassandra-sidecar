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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Closeable;
import io.vertx.core.Future;
import io.vertx.core.Promise;
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
    private static final long PLACEHOLDER_TIMER_ID = -1L; // used when the run start immediately, not scheduled via a timer

    // keep track of the timerIds in order to cancel them when closing/unscheduling
    private final Map<PeriodicTaskKey, Long> timerIds = new ConcurrentHashMap<>();
    private final Map<PeriodicTaskKey, Future<Void>> activeRuns = new ConcurrentHashMap<>();
    private final Set<PeriodicTaskKey> poisonPilledTasks = ConcurrentHashMap.newKeySet();
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
        AtomicBoolean runImmediately = new AtomicBoolean(actualDelayMillis <= 0);
        timerIds.compute(key, (k, v) -> {
            // The periodic task has been scheduled already. Exit early and avoid scheduling the duplication
            if (v != null && execCount == 0)
            {
                LOGGER.debug("Task is already scheduled. task='{}'", key);
                runImmediately.set(false);
                return v;
            }

            LOGGER.debug("Scheduling task {}. task='{}' execCount={}",
                         runImmediately.get() ? "immediately" : "in " + actualDelayMillis + " milliseconds",
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
            if (runImmediately.get())
            {
                return PLACEHOLDER_TIMER_ID; // use the placeholder ID, since this run is not scheduled as a timer
            }
            // Schedule and update the timer id
            return internalPool.setTimer(delayMillis, timerId -> executeAndScheduleNext(key, execCount));
        });

        if (runImmediately.get())
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
        long startTime = System.nanoTime();
        internalPool.<Void>executeBlocking(promise -> executeInternal(promise, key, execCount), false)
                    .onComplete(ignored -> {
                        LOGGER.debug("Task run finishes. task='{}' execCount={}", key, execCount);
                        // schedule the next run iff the task is not killed
                        if (poisonPilledTasks.remove(key))
                        {
                            // timerId might get populated after unschedule due to race.
                            // Have another attempt to clean up here.
                            timerIds.remove(key);
                            LOGGER.debug("Avoid scheduling the next run, and remove it from poisonPilledTasks. task='{}' execCount={}",
                                         key, execCount);
                            return;
                        }
                        long priorExecutionDurationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
                        schedule(key, priorExecutionDurationMillis, key.task.delay().to(TimeUnit.MILLISECONDS), execCount + 1);
                    });
    }

    /**
     * Unschedule and close the {@link PeriodicTask} iff it has been scheduled.
     *
     * @param task the task to unschedule
     */
    public void unschedule(PeriodicTask task)
    {
        unschedule(task, true);
    }

    /**
     * Unschedule the {@link PeriodicTask} iff it has been scheduled.
     *
     * @param task the {@link PeriodicTask} to unschedule
     * @param shouldCloseTask indicate whether {@link PeriodicTask#close()} should be called
     * @return a future of unschedule
     */
    private Future<Void> unschedule(PeriodicTask task, boolean shouldCloseTask)
    {
        PeriodicTaskKey key = new PeriodicTaskKey(task);
        Long timerId = timerIds.remove(key);
        if (timerId == null)
        {
            return Future.failedFuture("No such PeriodicTask: " + key);
        }

        LOGGER.debug("Unscheduling task. task='{}' timerId={}", key, timerId);
        // always insert a poison pill when unscheduling an existing task,
        // and conditionally remove when the timer can be cancelled
        poisonPilledTasks.add(key);
        // if timer is not started, it can be cancelled
        if (timerId != PLACEHOLDER_TIMER_ID && internalPool.cancelTimer(timerId))
        {
            poisonPilledTasks.remove(key);
        }

        // The current run might have started already.
        // If so, a non-null activeRun should be retrieved and a poison pill
        // is placed to avoid further scheduling.
        // Reschedule should only happen after the activeRun finishes.
        Future<Void> unscheduleFuture = activeRuns.getOrDefault(key, Future.succeededFuture());
        return shouldCloseTask
               ? unscheduleFuture.onComplete(ignored -> task.close())
               : unscheduleFuture;
    }


    /**
     * Reschedules the provided {@code task}.
     * <p>The difference from {@link #unschedule(PeriodicTask)} then {@link #schedule(PeriodicTask)}
     * is that the {@link PeriodicTask} in question is not closed when unscheduling
     *
     * @param task the task to reschedule
     */
    public void reschedule(PeriodicTask task)
    {
        unschedule(task, false)
        .onComplete(ignored -> schedule(task));
    }

    @Override
    public void close(Promise<Void> completion)
    {
        try
        {
            timerIds.values().forEach(internalPool::cancelTimer);
            timerIds.keySet().forEach(key -> key.task.close());
            timerIds.clear();
            completion.complete();
        }
        catch (Throwable throwable)
        {
            completion.fail(throwable);
        }
    }

    private void executeInternal(Promise<Void> promise, PeriodicTaskKey key, long execCount)
    {
        PeriodicTask periodicTask = key.task;
        switch (consolidateScheduleDecision(periodicTask))
        {
            case SKIP:
                LOGGER.trace("Skip executing task. task='{}' execCount={}", key, execCount);
                promise.tryComplete();
                return;

            case EXECUTE:
                break;

            case RESCHEDULE:
            default:
                LOGGER.debug("Reschedule the task. task='{}' execCount={}", key, execCount);
                reschedule(periodicTask);
                promise.tryComplete();
                return;
        }

        // Leverage a separate promise, taskRunPromise, to ensure the activeRun removal
        // happens before the completion of the promise of executeBlocking and its subsequent calls
        Promise<Void> taskRunPromise = Promise.promise();
        Future<Void> futureResult = taskRunPromise.future().onComplete(res -> {
            LOGGER.debug("Removing task from active runs. task='{}' execCount={}", key, execCount);
            activeRuns.remove(key);
            // finally complete the promise of executeBlocking
            promise.handle(res);
        });
        try
        {
            LOGGER.debug("Executing task. task='{}' execCount={}", key, execCount);
            activeRuns.put(key, futureResult);
            periodicTask.execute(taskRunPromise);
        }
        catch (Throwable throwable)
        {
            LOGGER.warn("Periodic task failed to execute. task='{}' execCount={}", periodicTask.name(), execCount, throwable);
            taskRunPromise.tryFail(throwable);
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
    Set<PeriodicTaskKey> poisonPilledTasks()
    {
        return poisonPilledTasks;
    }
}
