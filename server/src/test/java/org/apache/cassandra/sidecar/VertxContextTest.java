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

package org.apache.cassandra.sidecar;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.vertx.core.Context;
import io.vertx.core.Vertx;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test cases demonstrating the Vert.x threading model, especially thread switching
 * with shared futures.
 * <p>
 * These tests illustrate the concepts from docs/vertx-threading-model.md
 */
@Disabled("Excluded from CI. The test class is for demonstration purpose. Comment me out to run the tests")
public class VertxContextTest
{
    private Vertx vertx;
    private ExecutorService externalExecutor;

    @BeforeEach
    void setUp()
    {
        vertx = Vertx.vertx();
        externalExecutor = Executors.newFixedThreadPool(2);
    }

    @AfterEach
    void tearDown()
    {
        TestResourceReaper.create().with(vertx).with(externalExecutor::shutdown).close();
    }

    /**
     * Demonstrates basic context capture and verification that code executes
     * on the expected event loop thread.
     * <p>
     * This test shows:
     * - How to capture the current Vert.x context
     * - How to verify you're running on an event loop thread
     * - That each event loop has a unique context
     */
    @Test
    void testBasicContextCapture() throws Exception
    {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Context> capturedContext = new AtomicReference<>();
        AtomicReference<Thread> capturedThread = new AtomicReference<>();

        vertx.runOnContext(v -> {
            // Capture context and thread when running on event loop
            capturedContext.set(Vertx.currentContext());
            capturedThread.set(Thread.currentThread());

            // Verify we're on an event loop thread
            assertThat(capturedContext.get()).isNotNull();
            assertThat(capturedContext.get().isEventLoopContext()).isTrue();
            assertThat(Thread.currentThread().getName()).startsWith("vert.x-eventloop-thread");

            latch.countDown();
        });

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(capturedContext.get()).isNotNull();
        assertThat(capturedThread.get().getName()).startsWith("vert.x-eventloop-thread");
    }

    /**
     * Demonstrates the BROKEN pattern: thread switching with shared CompletableFuture.
     * <p>
     * This test illustrates the problem from CASSSIDECAR-368:
     * - Request starts on event-loop-A
     * - Shared CompletableFuture completes on external thread or different event loop
     * - Continuation executes on the completing thread (NOT event-loop-A)
     * - This violates the Golden Rule and can cause race conditions
     */
    @Test
    void testSharedFutureThreadSwitching() throws Exception
    {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Thread> originalThread = new AtomicReference<>();
        AtomicReference<Thread> continuationThread = new AtomicReference<>();
        AtomicReference<Context> originalContext = new AtomicReference<>();
        AtomicReference<Context> continuationContext = new AtomicReference<>();

        // Simulate a shared future that multiple requests might access
        // (like a cached authentication future)
        CompletableFuture<String> sharedFuture = new CompletableFuture<>();

        // Complete the future from an external thread (simulating external async operation)
        externalExecutor.submit(() -> {
            try
            {
                Thread.sleep(50); // Simulate async work
                sharedFuture.complete("auth-token");
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        });

        // Simulate request handler on event loop A
        vertx.runOnContext(v -> {
            originalThread.set(Thread.currentThread());
            originalContext.set(Vertx.currentContext());

            // BROKEN: Continuation runs on whichever thread completes the future
            sharedFuture.thenAccept(token -> {
                continuationThread.set(Thread.currentThread());
                continuationContext.set(Vertx.currentContext());

                // This demonstrates the problem: we've switched threads!
                assertThat(continuationThread.get()).isNotEqualTo(originalThread.get());

                // The context might also be different or null
                // This is the core issue that caused CASSSIDECAR-368
                latch.countDown();
            });
        });

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        // Verify thread switching occurred
        assertThat(originalThread.get()).isNotNull();
        assertThat(continuationThread.get()).isNotNull();
        assertThat(originalThread.get().getName()).startsWith("vert.x-eventloop-thread");
        // The continuation thread is NOT the same as original
        assertThat(continuationThread.get()).isNotEqualTo(originalThread.get());
    }

    /**
     * Demonstrates the FIXED pattern: proper context restoration with runOnContext.
     * <p>
     * This test shows the correct approach and the core fix for CASSSIDECAR-368:
     * - Capture the original context at request entry
     * - When external async operation completes, dispatch back to original context
     * - All request handling code runs on the same event loop thread
     */
    @Test
    void testProperContextRestoration() throws Exception
    {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Thread> originalThread = new AtomicReference<>();
        AtomicReference<Thread> restoredThread = new AtomicReference<>();
        AtomicReference<Context> originalContext = new AtomicReference<>();
        AtomicReference<Context> restoredContext = new AtomicReference<>();

        // Shared future that completes on external thread
        CompletableFuture<String> sharedFuture = new CompletableFuture<>();

        externalExecutor.submit(() -> {
            try
            {
                Thread.sleep(50);
                sharedFuture.complete("auth-token");
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        });

        // Simulate request handler on event loop
        vertx.runOnContext(v -> {
            // Step 1: Capture context at handler entry
            Context originCtx = Vertx.currentContext();
            originalThread.set(Thread.currentThread());
            originalContext.set(originCtx);

            // Step 2: When external async completes, restore context before touching request
            sharedFuture.thenAccept(token -> {
                originCtx.runOnContext(ignored -> {
                    // Now we're back on the original event loop thread
                    restoredThread.set(Thread.currentThread());
                    restoredContext.set(Vertx.currentContext());

                    // FIXED: Same thread and context as original
                    assertThat(restoredThread.get()).isEqualTo(originalThread.get());
                    assertThat(restoredContext.get()).isEqualTo(originalContext.get());

                    latch.countDown();
                });
            });
        });

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        // Verify we stayed on the same thread throughout request handling
        assertThat(originalThread.get()).isNotNull();
        assertThat(restoredThread.get()).isNotNull();
        assertThat(originalThread.get()).isEqualTo(restoredThread.get());
        assertThat(originalContext.get()).isEqualTo(restoredContext.get());
    }

    /**
     * Demonstrates multiple sequential async operations with proper context restoration.
     * <p>
     * This test shows:
     * - Context must be restored after EACH external async operation
     * - The pattern works for chained operations
     * - All request handling code stays on the same event loop thread
     */
    @Test
    void testMultipleAsyncOperationsWithContextRestoration() throws Exception
    {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Thread> originalThread = new AtomicReference<>();
        AtomicReference<Thread> afterFirstOp = new AtomicReference<>();
        AtomicReference<Thread> afterSecondOp = new AtomicReference<>();

        vertx.runOnContext(v -> {
            Context originCtx = Vertx.currentContext();
            originalThread.set(Thread.currentThread());

            // First external async operation
            CompletableFuture<String> firstOp = CompletableFuture.supplyAsync(
                () -> "first-result",
                externalExecutor
            );

            firstOp.thenAccept(firstResult -> {
                // Restore context after first operation
                originCtx.runOnContext(ignored1 -> {
                    afterFirstOp.set(Thread.currentThread());

                    // Second external async operation
                    CompletableFuture<String> secondOp = CompletableFuture.supplyAsync(
                        () -> "second-result",
                        externalExecutor
                    );

                    secondOp.thenAccept(secondResult -> {
                        // Restore context after second operation
                        originCtx.runOnContext(ignored2 -> {
                            afterSecondOp.set(Thread.currentThread());

                            // All operations executed on the same event loop thread
                            assertThat(afterFirstOp.get()).isEqualTo(originalThread.get());
                            assertThat(afterSecondOp.get()).isEqualTo(originalThread.get());

                            latch.countDown();
                        });
                    });
                });
            });
        });

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        // Verify all operations stayed on the same thread
        assertThat(originalThread.get()).isNotNull();
        assertThat(afterFirstOp.get()).isEqualTo(originalThread.get());
        assertThat(afterSecondOp.get()).isEqualTo(originalThread.get());
    }
}
