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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import static org.apache.cassandra.sidecar.concurrent.AsyncConcurrentTaskExecutor.TASK_CANCEL_MESSAGE;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
class AsyncConcurrentTaskExecutorTest
{
    private final Vertx vertx = Vertx.vertx();

    @Test
    public void testTaskFailure(VertxTestContext context)
    {
        List<Callable<Future<Boolean>>> tasks = new ArrayList<>();
        String failureMessage = "Task failed";
        tasks.add(getDummyFailedTask(failureMessage));
        AsyncConcurrentTaskExecutor<Boolean> concurrentTaskExecutor = new AsyncConcurrentTaskExecutor<>(vertx, tasks, 1);
        List<Future<Boolean>> taskFutures = concurrentTaskExecutor.start();
        Future.join(taskFutures).onComplete(context.failing(result -> context.verify(() -> {
            assertThat(result).isNotNull();
            assertThat(result.getMessage()).isEqualTo(failureMessage);
            assertThat(taskFutures.get(0).failed()).isTrue();
            context.completeNow();
        })));
    }

    @Test
    void testZeroTasks(VertxTestContext context)
    {
        List<Callable<Future<Boolean>>> tasks = new ArrayList<>();
        AsyncConcurrentTaskExecutor<Boolean> concurrentTaskExecutor = new AsyncConcurrentTaskExecutor<>(vertx, tasks, 1);
        List<Future<Boolean>> taskFutures = concurrentTaskExecutor.start();
        Future.join(taskFutures).onComplete(ar -> context.verify(() -> {
            assertThat(ar).isNotNull();
            assertThat(ar.result().list().size()).isEqualTo(0);
            context.completeNow();
        }));
    }

    @Test
    public void testMultipleTaskFailure(VertxTestContext context)
    {
        List<Callable<Future<Boolean>>> tasks = new ArrayList<>();
        for (int i = 0; i < 10; i++)
        {
            String failureMessage = "Task " + i + " failed.";
            tasks.add(getDummyFailedTask(failureMessage));
        }
        AsyncConcurrentTaskExecutor<Boolean> concurrentTaskExecutor = new AsyncConcurrentTaskExecutor<>(vertx, tasks, 5);
        List<Future<Boolean>> taskFutures = concurrentTaskExecutor.start();
        Future.join(taskFutures).onComplete(context.failing(result -> context.verify(() -> {
            assertThat(result).isNotNull();
            taskFutures.forEach(ar -> assertThat(ar.failed()).isTrue());
            context.completeNow();
        })));
    }

    @Test
    public void testSingleTaskSuccess(VertxTestContext context)
    {
        List<Callable<Future<String>>> tasks = Collections.singletonList(getDummySuccessTask("Task executed successfully"));
        AsyncConcurrentTaskExecutor<String> concurrentTaskExecutor = new AsyncConcurrentTaskExecutor<>(vertx, tasks, 1);
        List<Future<String>> taskFutures = concurrentTaskExecutor.start();
        Future.join(taskFutures).onComplete(ar -> context.verify(() -> {
            assertThat(ar).isNotNull();
            assertThat(ar.result().list().get(0)).isEqualTo("Task executed successfully");
            context.completeNow();
        }));
    }

    @Test
    public void testMultipleTasksSucceeds(VertxTestContext context)
    {
        List<Callable<Future<Boolean>>> tasks = getDummySuccessTasks(10, Boolean.TRUE);
        AsyncConcurrentTaskExecutor<Boolean> concurrentTaskExecutor = new AsyncConcurrentTaskExecutor<>(vertx, tasks, 5);
        List<Future<Boolean>> taskFutures = concurrentTaskExecutor.start();
        Future.join(taskFutures).onComplete(ar -> context.verify(() -> {
            assertThat(ar).isNotNull();
            for (Future<Boolean> taskFuture : taskFutures)
            {
                assertThat(taskFuture.succeeded()).isTrue();
                assertThat(taskFuture.result()).isTrue();
            }
            context.completeNow();
        }));
    }

    @Test
    public void testLotsOfTasks(VertxTestContext context)
    {
        List<Callable<Future<Integer>>> tasks = getDummySuccessTasks(100_000, 1);
        AsyncConcurrentTaskExecutor<Integer> concurrentTaskExecutor = new AsyncConcurrentTaskExecutor<>(vertx, tasks, 50);
        List<Future<Integer>> taskFutures = concurrentTaskExecutor.start();
        Future.join(taskFutures).onComplete(ar -> context.verify(() -> {
            assertThat(ar).isNotNull();
            for (Future<Integer> taskFuture : taskFutures)
            {
                assertThat(taskFuture.succeeded()).isTrue();
                assertThat(taskFuture.result()).isEqualTo(1);
            }
            context.completeNow();
        }));
    }

    @Test
    public void testTaskCancel(VertxTestContext context)
    {
        Promise<Boolean> promise = Promise.promise();
        List<Callable<Future<Boolean>>> tasks = getDummyPendingTasks(10, promise);
        AsyncConcurrentTaskExecutor<Boolean> concurrentTaskExecutor = new AsyncConcurrentTaskExecutor<>(vertx, tasks, 1);
        List<Future<Boolean>> taskFutures = concurrentTaskExecutor.start();

        concurrentTaskExecutor.cancelTasks();

        // Complete promise so that first task gets finished
        promise.fail(new Exception("ERROR!"));

        Future.join(taskFutures).onComplete(context.failing(result -> context.verify(() -> {
            assertThat(result).isNotNull();

            Future<Boolean> firstTask = taskFutures.get(0);
            assertThat(firstTask.failed()).isTrue();
            assertThat(firstTask.cause()).isInstanceOf(Exception.class);

            for (int i = 1; i < taskFutures.size(); i++)
            {
                Future<Boolean> task = taskFutures.get(i);
                assertThat(task.failed()).isTrue();
                assertThat(task.cause().getMessage()).isEqualTo(TASK_CANCEL_MESSAGE);
                assertThat(task.cause()).isInstanceOf(CancellationException.class);
            }

            context.completeNow();
        })));
    }

    @Test
    public void testCancelLotOfTasks(VertxTestContext context)
    {
        Promise<Integer> promise = Promise.promise();
        List<Callable<Future<Integer>>> tasks = getDummyPendingTasks(10_000, promise);
        AsyncConcurrentTaskExecutor<Integer> taskExecutor = new AsyncConcurrentTaskExecutor<>(vertx, tasks, 1);
        List<Future<Integer>> taskFutures = taskExecutor.start();

        taskExecutor.cancelTasks();

        // Resolve the promise so that first task will finish
        promise.complete(0);

        Future.join(taskFutures).onComplete(context.failing(result -> context.verify(() -> {
            assertThat(result).isNotNull();

            Future<Integer> firstTask = taskFutures.get(0);
            assertThat(firstTask.isComplete()).isTrue();
            assertThat(firstTask.succeeded()).isTrue();
            assertThat(firstTask.result()).isEqualTo(0);

            for (int i = 1; i < taskFutures.size(); i++)
            {
                Future<Integer> task = taskFutures.get(i);
                assertThat(task.failed()).isTrue();
                assertThat(task.cause().getMessage()).isEqualTo(TASK_CANCEL_MESSAGE);
                assertThat(task.cause()).isInstanceOf(CancellationException.class);
            }

            context.completeNow();
        })));
    }

    @Test
    public void testCancelWhenTasksInProgress()
    {
        Promise<Integer> task1Promise = Promise.promise();
        Callable<Future<Integer>> task1 = () -> task1Promise.future()
                                                            .compose(res -> Future.succeededFuture(0));
        Callable<Future<Integer>> task2 = getDummySuccessTask(1);
        Promise<Integer> task3Promise = Promise.promise();
        Callable<Future<Integer>> task3 = () -> task3Promise.future()
                                                            .compose(res -> Future.succeededFuture(2));
        Callable<Future<Integer>> task4 = getDummySuccessTask(3);
        Callable<Future<Integer>> task5 = getDummySuccessTask(4);

        List<Callable<Future<Integer>>> tasks = Arrays.asList(task1, task2, task3, task4, task5);
        AsyncConcurrentTaskExecutor<Integer> concurrentTaskExecutor = new AsyncConcurrentTaskExecutor<>(vertx, tasks, 2);
        List<Future<Integer>> taskFutures = concurrentTaskExecutor.start();

        // Wait for some time so that independent tasks get time finish.
        CompositeFuture compositeFuture = waitForTasks(taskFutures, 1_000);
        assertThat(compositeFuture.isComplete()).isFalse();
        concurrentTaskExecutor.cancelTasks();
        waitForTasks(compositeFuture, 1_000);

        assertThat(taskFutures.get(0).succeeded()).isFalse();
        assertThat(taskFutures.get(0).isComplete()).isFalse();

        assertThat(taskFutures.get(1).succeeded()).isTrue();
        assertThat(taskFutures.get(1).isComplete()).isTrue();

        assertThat(taskFutures.get(2).succeeded()).isFalse();
        assertThat(taskFutures.get(2).isComplete()).isFalse();

        assertThat(taskFutures.get(3).isComplete()).isTrue();
        assertThat(taskFutures.get(3).failed()).isTrue();
        assertThat(taskFutures.get(3).cause()).isInstanceOf(CancellationException.class);

        assertThat(taskFutures.get(4).failed()).isTrue();
        assertThat(taskFutures.get(4).isComplete()).isTrue();
        assertThat(taskFutures.get(4).cause()).isInstanceOf(CancellationException.class);

        assertThat(compositeFuture.isComplete()).isFalse();

        task1Promise.complete();
        task3Promise.complete();

        waitForTasks(compositeFuture, 2_000);
        assertThat(Future.join(taskFutures).isComplete()).isTrue();
    }

    @Test
    public void testMaxConcurrency(VertxTestContext context)
    {
        AtomicInteger counter = new AtomicInteger(0);
        int maxConcurrency = 4;
        List<Callable<Future<Integer>>> tasks = new ArrayList<>();
        for (int i = 0; i < 1000; i++)
        {
            Callable<Future<Integer>> task = getDummySuccessCounterTask(counter);
            tasks.add(task);
        }
        TestAsyncTaskExecutor<Integer> concurrentTaskExecutor =
        new TestAsyncTaskExecutor<>(vertx, tasks, maxConcurrency, counter);
        List<Future<Integer>> taskFutures = concurrentTaskExecutor.start();

        Future.join(taskFutures).onComplete(ar -> context.verify(() -> {
            assertThat(ar).isNotNull();
            assertThat(concurrentTaskExecutor.getMax()).isEqualTo(maxConcurrency);
            context.completeNow();
        }));
    }

    @Test
    public void testMixOfSuccessAndFailedTasks(VertxTestContext context)
    {
        List<Callable<Future<String>>> tasks = new ArrayList<>();
        tasks.add(getDummySuccessTask("success-1"));
        tasks.add(getDummyFailedTask("failure-1"));
        tasks.add(getDummySuccessTask("success-2"));
        tasks.add(getDummyFailedTask("failure-2"));
        tasks.add(getDummySuccessTask("success-3"));

        AsyncConcurrentTaskExecutor<String> concurrentTaskExecutor = new AsyncConcurrentTaskExecutor<>(vertx, tasks, 3);
        List<Future<String>> taskFutures = concurrentTaskExecutor.start();

        Future.join(taskFutures).onComplete(context.failing(result -> context.verify(() -> {
            assertThat(result).isNotNull();

            // Verify success tasks
            assertThat(taskFutures.get(0).succeeded()).isTrue();
            assertThat(taskFutures.get(0).result()).isEqualTo("success-1");
            assertThat(taskFutures.get(2).succeeded()).isTrue();
            assertThat(taskFutures.get(2).result()).isEqualTo("success-2");
            assertThat(taskFutures.get(4).succeeded()).isTrue();
            assertThat(taskFutures.get(4).result()).isEqualTo("success-3");

            // Verify failed tasks
            assertThat(taskFutures.get(1).failed()).isTrue();
            assertThat(taskFutures.get(1).cause().getMessage()).isEqualTo("failure-1");
            assertThat(taskFutures.get(3).failed()).isTrue();
            assertThat(taskFutures.get(3).cause().getMessage()).isEqualTo("failure-2");

            context.completeNow();
        })));
    }

    @Test
    public void testTaskThrowsException(VertxTestContext context)
    {
        List<Callable<Future<String>>> tasks = new ArrayList<>();
        String exceptionMessage = "Task threw exception";

        tasks.add(() -> {
            throw new RuntimeException(exceptionMessage);
        });

        AsyncConcurrentTaskExecutor<String> concurrentTaskExecutor = new AsyncConcurrentTaskExecutor<>(vertx, tasks, 1);
        List<Future<String>> taskFutures = concurrentTaskExecutor.start();

        Future.join(taskFutures).onComplete(context.failing(result -> context.verify(() -> {
            assertThat(result).isNotNull();
            assertThat(taskFutures.get(0).failed()).isTrue();
            assertThat(taskFutures.get(0).cause()).isInstanceOf(RuntimeException.class);
            assertThat(taskFutures.get(0).cause().getMessage()).isEqualTo(exceptionMessage);
            context.completeNow();
        })));
    }

    @Test
    public void testWithMaxConcurrencyOne(VertxTestContext context)
    {

        // Create tasks with delays to ensure they execute in order
        List<Promise<Integer>> taskControlPromises = new ArrayList<>();
        List<Future<Integer>> taskCompletionFutures = new ArrayList<>();
        List<Callable<Future<Integer>>> tasks = new ArrayList<>();

        for (int i = 0; i < 5; i++)
        {
            Promise<Integer> controlPromise = Promise.promise();
            Promise<Integer> completionPromise = Promise.promise();
            taskControlPromises.add(controlPromise);
            taskCompletionFutures.add(completionPromise.future());

            final int taskIndex = i;
            tasks.add(() -> {
                // Task will wait for its control promise before completing
                return controlPromise.future().compose(ignored -> {
                    completionPromise.complete(taskIndex);
                    return Future.succeededFuture(taskIndex);
                });
            });
        }

        AsyncConcurrentTaskExecutor<Integer> executor = new AsyncConcurrentTaskExecutor<>(vertx, tasks, 1);
        List<Future<Integer>> taskFutures = executor.start();

        // Only the first task should be running, complete it and verify the second starts
        vertx.setTimer(100, id -> {
            context.verify(() -> {
                // Complete the first task
                taskControlPromises.get(0).complete(0);
            });

            // Wait for the first task to complete and second to start
            vertx.setTimer(100, id2 -> {
                context.verify(() -> {
                    assertThat(taskCompletionFutures.get(0).isComplete()).isTrue();

                    // Complete the second task
                    taskControlPromises.get(1).complete(1);
                });

                // Complete remaining tasks
                vertx.setTimer(100, id3 -> {
                    context.verify(() -> {
                        for (int i = 2; i < taskControlPromises.size(); i++)
                        {
                            taskControlPromises.get(i).complete(i);
                        }
                    });

                    // Verify all tasks complete in order
                    Future.join(taskFutures).onComplete(ar -> context.verify(() -> {
                        for (int i = 0; i < taskFutures.size(); i++)
                        {
                            assertThat(taskFutures.get(i).result()).isEqualTo(i);
                        }
                        context.completeNow();
                    }));
                });
            });
        });
    }

    private <T> Callable<Future<T>> getDummyFailedTask(String failureMessage)
    {
        return () -> Future.failedFuture(failureMessage);
    }

    private <T> List<Callable<Future<T>>> getDummySuccessTasks(int numTasks, T successValue)
    {
        List<Callable<Future<T>>> successTasks = new ArrayList<>();
        for (int i = 0; i < numTasks; i++)
        {
            successTasks.add(getDummySuccessTask(successValue));
        }
        return successTasks;
    }

    private <T> List<Callable<Future<T>>> getDummyPendingTasks(int numTasks, Promise<T> promise)
    {
        List<Callable<Future<T>>> pendingTasks = new ArrayList<>();
        for (int i = 0; i < numTasks; i++)
        {
            pendingTasks.add(promise::future);
        }
        return pendingTasks;
    }

    @SuppressWarnings("SameParameterValue")
    private <T> CompositeFuture waitForTasks(List<Future<T>> taskFutures, long maxWaitTimeMillis)
    {
        return waitForTasks(Future.join(taskFutures), maxWaitTimeMillis);
    }

    private CompositeFuture waitForTasks(CompositeFuture compositeFuture, long maxWaitTimeMillis)
    {
        long waitTill = System.currentTimeMillis() + maxWaitTimeMillis;
        //noinspection StatementWithEmptyBody
        while (System.currentTimeMillis() <= waitTill && !compositeFuture.isComplete()) ;
        return compositeFuture;
    }

    private <T> Callable<Future<T>> getDummySuccessTask(T successValue)
    {
        return () -> Future.succeededFuture(successValue);
    }

    private Callable<Future<Integer>> getDummySuccessCounterTask(AtomicInteger counter)
    {
        return () -> {
            Promise<Integer> p = Promise.promise();
            Future<Integer> future = p.future();
            vertx.setTimer(10, tid -> p.complete(counter.decrementAndGet()));
            return future;
        };
    }

    static class TestAsyncTaskExecutor<T> extends AsyncConcurrentTaskExecutor<T>
    {
        AtomicInteger counter;
        int max = 0;

        public TestAsyncTaskExecutor(Vertx vertx,
                                     List<Callable<Future<T>>> tasks,
                                     int maxConcurrency,
                                     AtomicInteger counter)
        {
            super(vertx, tasks, maxConcurrency);
            this.counter = counter;
        }

        @Override
        public List<Future<T>> start()
        {
            List<Promise<T>> taskPromises = super.getTaskPromises();
            for (Promise<T> p : taskPromises)
            {
                p.future().onComplete(ar -> {
                    int val = counter.incrementAndGet();
                    max = Math.max(max, val);
                });
            }
            return super.start();
        }

        int getMax()
        {
            return max;
        }
    }
}
