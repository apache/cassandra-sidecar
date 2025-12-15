<!--
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
-->

# Vert.x Threading Model Guide

## The Golden Rule

**Each HTTP request MUST be processed by exactly ONE Vert.x event loop thread throughout its entire lifecycle.**

Vert.x makes optimizations based on this assumption. Violating it breaks operation ordering and causes race conditions.

## Real Example: CASSSIDECAR-368 SSTable Upload Corruption

### The Bug: Data Loss from Threading Violation

When request processing jumped between event loops, 8 KiB chunks at the beginning of files were lost:

**Sequence of events (WRONG - operations split across 2 threads):**

```
T1: Request pause scheduled in event-loop A
T2: After buffering 8 chunks, connection finally paused (pause is async)
T3: Shared authFuture completes → continuation runs in event-loop B  ← THREAD JUMP
T4: Request resume scheduled from event-loop B (queued on event-loop A)
T5: Event-loop A drains data, but data handler NOT installed yet
    → eventHandler is null
    → All chunks DISCARDED (8 KiB multiples missing)
T6: Event-loop B continues, enters SSTableUploadHandler, attaches data handler (too late)
```

**After fix (CORRECT - all operations on same thread):**

```
T1: Request pause scheduled in event-loop A
T2: After buffering 8 chunks, connection finally paused
T3: Shared authFuture completes → continuation scheduled back to event-loop A  ← STAYS ON A
T4: Enter SSTableUploadHandler, pause request (scheduled)
T5: PipeImpl.to attaches data handler and schedules resume
T6: Data drains and handler receives all chunks ✓
```

## Understanding Vert.x Contexts

Most applications don't need tight interactions with a context, but sometimes it's useful to access them, especially when your application uses another library that performs a callback on its own thread and you want to execute code in the original context.

### Accessing the Current Context

You can get the current context in two ways:

```java
// Option 1: Always returns a context (creates one if needed)
Context context = vertx.getOrCreateContext();

// Option 2: Returns null if current thread is not associated with a context
Context context = Vertx.currentContext();
```

The first method never returns null and will create a context if needed. The second method might return null if the current thread is not associated with a context.

### Using Context to Run Code

After obtaining a context, you can use it to run code in that context:

```java
public void integrateWithExternalSystem(Handler<Event> handler) {
    // Capture the current context
    Context context = vertx.getOrCreateContext();

    // Run the event handler on the application context
    externalSystem.onEvent(event -> {
        context.runOnContext(v -> handler.handle(event));
    });
}
```

In practice, many Vert.x APIs and third-party libraries are implemented this way to ensure callbacks execute on the correct event loop thread.

## The Pattern

```java
public void handle(RoutingContext context) {
    Context originCtx = Vertx.currentContext();  // 1. Capture context at handler entry

    externalAsyncOperation()
    .onSuccess(result -> {
        originCtx.runOnContext(v -> {  // 2. Restore context before touching request
            context.next();
        });
    })
    .onFailure(cause -> {
        originCtx.runOnContext(v -> {  // 3. Restore in ALL branches
            context.fail(500, cause);
        });
    });
}
```

## When You Need This Pattern

**Required** when handling HTTP requests and integrating external async APIs:

- **Caffeine Cache** with `Future` values - even synchronous `Cache<K, Future<V>>`
  - Cached futures complete on different threads depending on when they were created
  - Shared futures from concurrent requests can jump threads
- **AWS SDK** - `CompletableFuture` completes on SDK thread pool
- **Any `CompletableFuture`-based library** that you use in request handlers

**Not required** for:
- **Vert.x native async operations** (already maintain context): `vertx.executeBlocking()`, `vertx.fileSystem().*`, `WebClient`, timers, event bus
- **Background tasks** not part of request handling (e.g., periodic maintenance, metrics collection, async cleanup tasks)

If you're not accessing request state (`RoutingContext`, `HttpServerRequest`, response), you don't need to dispatch back to the original event-loop thread.

## References

- CASSSIDECAR-368: Fix request execution continues on wrong thread
- Vert.x Dealing with Contexts: https://vertx.io/docs/guides/advanced-vertx-guide/#_dealing_with_contexts
- Vert.x Event Loop Context: https://vertx.io/docs/guides/advanced-vertx-guide/#_event_loop_context
- Vert.x Golden Rule: https://vertx.io/docs/vertx-core/java/#golden_rule
