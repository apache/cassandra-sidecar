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

package org.apache.cassandra.sidecar.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.jetbrains.annotations.NotNull;

/**
 * A task executor that manages concurrent execution of asynchronous tasks with configurable concurrency limit.
 *
 * <h2>Overview</h2>
 * <p>This executor ensures that only a specified number of tasks (defined by {@code maxConcurrency})
 * execute simultaneously. When a task completes, the next pending task is automatically
 * triggered, maintaining optimal resource utilization while respecting concurrency constraints.</p>
 *
 * <h2>Architecture</h2>
 * <p>The executor uses a promise-based triggering mechanism:</p>
 * <ul>
 *   <li>Each task is associated with a {@link Promise} that acts as a trigger signal</li>
 *   <li>Tasks only start execution when their corresponding Promise completes successfully</li>
 *   <li>Initially, {@code maxConcurrency} number of promises are completed to start the execution flow</li>
 *   <li>As tasks complete, the next pending task is automatically triggered</li>
 * </ul>
 *
 * <h2>Concurrency Control</h2>
 * <p>The executor maintains a global task index using {@link AtomicInteger} to ensure thread-safe
 * task scheduling. Tasks are processed in the order they were submitted, with automatic
 * flow control to maintain the desired concurrency level.</p>
 *
 * <h2>Error Handling</h2>
 * <p>When a task completes (successfully or with a non-{@link CancellationException} error),
 * the executor automatically triggers the next pending task. Tasks that are cancelled do not
 * trigger subsequent tasks to prevent further executions.</p>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * List<Callable<Future<String>>> tasks = Arrays.asList(
 *     () -> downloadFile("file1.txt"),
 *     () -> downloadFile("file2.txt"),
 *     () -> downloadFile("file3.txt")
 * );
 *
 * AsyncConcurrentTaskExecutor<String> executor =
 *     new AsyncConcurrentTaskExecutor<>(vertx, tasks, 2); // max 2 tasks can execute concurrently
 *
 * List<Future<String>> results = executor.start();
 * }</pre>
 *
 * @param <T> the type of result produced by the tasks
 */
public class AsyncConcurrentTaskExecutor<T>
{
    protected static final String TASK_CANCEL_MESSAGE = "task cancelled";
    private final AtomicInteger index = new AtomicInteger(0);
    private final int maxConcurrency;
    private final Vertx vertx;
    private final List<Promise<T>> taskPromises;
    private final List<Callable<Future<T>>> tasks;

    /**
     * Creates a new AsyncConcurrentTaskExecutor.
     *
     * @param vertx          the Vert.x instance for context execution
     * @param tasks          the list of tasks to execute (must not be empty)
     * @param maxConcurrency the maximum number of tasks to execute concurrently (must be &gt; 0)
     * @throws IllegalArgumentException if maxConcurrency &lt;= 0 or tasks is empty
     */
    public AsyncConcurrentTaskExecutor(@NotNull Vertx vertx,
                                       @NotNull List<Callable<Future<T>>> tasks,
                                       int maxConcurrency)
    {
        Objects.requireNonNull(vertx, "vertx must not be null.");
        Objects.requireNonNull(tasks, "tasks must not be null.");
        if (maxConcurrency < 1)
        {
            throw (new IllegalArgumentException("maxConcurrency must be > 0"));
        }

        this.tasks = List.copyOf(tasks);
        this.maxConcurrency = maxConcurrency;
        this.vertx = vertx;
        taskPromises = tasks.stream()
                            .map(item -> Promise.<T>promise())
                            .collect(Collectors.toUnmodifiableList());
    }

    /**
     * Returns the list of task promises for testing purposes.
     *
     * @return unmodifiable list of task promises
     */
    @VisibleForTesting
    List<Promise<T>> getTaskPromises()
    {
        return taskPromises;
    }

    /**
     * Starts the execution of tasks with the configured concurrency limit.
     *
     * <p>This method immediately starts up to {@code maxConcurrency} tasks and returns
     * futures for all tasks. As running tasks complete, pending tasks are automatically
     * triggered to maintain the concurrency level.</p>
     *
     * Execution Flow Diagram:
     * <pre>
     *            index
     *             +
     *             |
     *             v
     * +----+----+-+--+----+------------+----+
     * | p0 | p1 | p2 | p3 |    ...     |pn-1|
     * +-+--+-+--+-+--+-+--+------------+-+--+
     *   |    |    |    |                 |
     *   |    |    +    +                 +
     *   |    |
     *   |    |    +    +                 +
     *   |    |    |    |                 |
     *   +    +    +    +                 +
     *   t0   t1   t2   t3                tn-1
     *  (r)  (r)
     * (r) = running
     * </pre>
     *
     * @return list of futures representing all tasks (same order as input)
     */
    public List<Future<T>> start()
    {
        List<Future<T>> taskFutures = new ArrayList<>(tasks.size());
        for (int i = 0; i < tasks.size(); i++)
        {
            Promise<T> p = taskPromises.get(i);
            taskFutures.add(waitAndDo(p.future(), tasks.get(i)));
        }

        // Start maxConcurrency no.of tasks
        for (int i = 0; i < maxConcurrency; i++)
        {
            triggerNextTask();
        }
        return taskFutures;
    }

    /**
     * Associates a task with its trigger promise and handles task execution and completion.
     *
     * <p>This method creates a future chain where the task only executes when the trigger
     * future completes. Upon task completion, it automatically triggers the next pending task.</p>
     *
     * @param future the trigger future that must complete before task execution
     * @param task   the task to execute when triggered
     * @return future representing the task's execution result
     */
    private Future<T> waitAndDo(Future<T> future, Callable<Future<T>> task)
    {
        return future.compose(ar -> {
                         try
                         {
                             return task.call();
                         }
                         catch (Exception e)
                         {
                             return Future.failedFuture(e);
                         }
                     })
                     .onComplete(ar -> {
                         if (ar.succeeded() || !(ar.cause() instanceof CancellationException))
                         {
                             triggerNextTask();
                         }
                     });
    }

    /**
     * Triggers the next pending task by completing its associated promise.
     *
     * <p>This method is called when a task completes (successfully or with error) to maintain
     * the desired concurrency level. It uses an atomic index to ensure thread-safe task scheduling.</p>
     *
     * <h3>Task Flow Control Diagram:</h3>
     * <pre>
     *                                     ...+
     *                                        |
     * +--------------------+                 |
     * |                    |                 |     index
     * | +-------------+    |                 |     +
     * | |             |    |                 |     |
     * | |             v    v                 v     v
     * | | +----+----+-+--+-+--+------------+-+--+
     * | | | p0 | p1 | p2 | p3 |    ...     |pn-1|
     * | | +-+--+-+--+-+--+-+--+------------+-+--+
     * | |   |    |    |    |                 |
     * | |   |    |    |    |                 |
     * | |   |    |    |    |                 |
     * | |   |    |    |    |                 |
     * | |   |    |    |    |                 |
     * | |   +    +    +    +                 +
     * | |   t0   t1   t2   t3                tn-1
     * | |  (d)  (d)  (d)  (d)               (d)
     * | |   +    +    +    +
     * | |   |    |    |    |
     * | +---+    |    |    |
     * +----------+    |    |
     *            ...  +    |
     *                  ... +
     * (d) = done
     * </pre>
     */
    void triggerNextTask()
    {
        int currentIndex = index.getAndIncrement();
        if (currentIndex < taskPromises.size())
        {
            vertx.runOnContext(ctx -> taskPromises.get(currentIndex).tryComplete());
        }
    }

    /**
     * Cancels all pending (not yet started) tasks by failing their trigger promises.
     *
     * <p>This method is thread-safe and can be called while tasks are executing.
     * Currently running tasks are not interrupted, but all pending tasks will be
     * cancelled with a {@link CancellationException}.</p>
     *
     * Cancellation Diagram:
     * <pre>
     *                                          index
     *                                          +
     *                                          |
     *                                          v
     * +----+----+----+----+------------+----+
     * | p0 | p1 | p2 | p3 |    ...     |pn-1|
     * +-+--+-+--+-+--+-+--+------------+-+--+
     *   |    |    |    |                 |
     *   |    |    +    +                 +
     *   |    |
     *   |    |    +    +                 +
     *   |    |    |    |                 |
     *   +    +    +    +                 +
     *   t0   t1   t2   t3                tn-1
     *  (r)  (r)  (c)  (c)               (c)
     * (r) = running, (c) = cancelled
     * </pre>
     */
    public synchronized void cancelTasks()
    {
        int currentIndex = index.getAndIncrement();
        while (currentIndex < taskPromises.size())
        {
            final int cancelIndex = currentIndex;
            vertx.runOnContext(ctx -> taskPromises.get(cancelIndex)
                                                  .tryFail(new CancellationException(TASK_CANCEL_MESSAGE)));
            currentIndex = index.getAndIncrement();
        }
    }
}
