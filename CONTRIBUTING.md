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

# Contributing to Apache Cassandra Sidecar

We warmly welcome and appreciate contributions from the community.

## Table of Contents

* [How to Contribute](#how-to-contribute)
  * [Discuss](#discuss)
  * [Create a Ticket](#ticket)
  * [Integration with IntelliJ IDEA](#idea)
  * [Testing](#testing)
* [Source Code Best Practices](#best-practices)
  * [Introducing new APIs](#new-apis)
  * [Asynchronous Programming](#async-programming)
  * [Thread Pool Model](#thread-pools)
  * [Vert.x Threading Model](#vertx-threading)
  * [One-shot Timers and Periodic Timers](#timers)
  * [Guice in Sidecar](#guice)
  * [Handler Chaining](#chaining-handlers)
  * [Asynchronous Handlers](#async-handlers)
  * [Future Composition](#future-composition)
  * [Failure Handling](#failure-handling)
  * [Cassandra Adapters](#cassandra-adapters)

## <a name="how-to-contribute"></a>How to Contribute

### <a name="discuss"></a>Discuss

Find an existing issue on Jira, or start a new discussing in the Apache Cassandra mailing list.

### <a name="ticket"></a>Create a Ticket

Before creating a ticket, please take the time to [research first](#discuss).

If you are creating a new Jira after a discussion on the Apache Cassandra mailing list, please provide a
self-sufficient description in the ticket. This is certainly extra work, but Jira is the place where we capture
important design discussions and decisions that can often be referenced after the fix version, to understand
the origin of a feature, understand design decisions, and so on.

When ready create a Jira ticket.

### <a name="idea"></a>Integration with IntelliJ IDEA

The project provides an
[IntelliJ IDEA code formatting configuration](https://github.com/apache/cassandra-sidecar/blob/trunk/ide/idea/codeStyleSettings.xml)
that defines the source file coding standards.

To import the formatting configuration run the following gradle task:

```shell
./gradlew idea
```

This will install the style settings into the `.idea` directory located at the root of the project directory.

You can then use the provided configuration that adheres to the project source file coding standards.

> **NOTE**: Opening a newly cloned repository in IDEA before running the command above will result in the default code
>           format settings being used instead; if that is the case, delete the `.idea` directory and start over.

### Testing

For comprehensive information about running tests, including unit tests, integration tests, and test configuration,
please refer to the [Testing Guide](TESTING.md). This guide covers:

- Test prerequisites and setup
- Different types of tests (unit, integration, container)
- Running tests with various configurations
- Troubleshooting common testing issues
- Development testing best practices

## <a name="best-practices"></a>Source Code Best Practices

The Apache Cassandra Sidecar project uses the [vertx](https://vertx.io) toolkit. It uses the asynchronous and
reactive programming paradigm. This allows for Sidecar to scale up as workloads grow, as well as resiliency when
failures arise, whereas the traditional one-request-per-thread threading model does not scale well beyond 
small-to-medium workloads.

To get yourself familiar with the codebase, a good starting point of exploration is the 
[modules](server/src/main/java/org/apache/cassandra/sidecar/modules). Check out each module and find out what are the relevant 
components of each feature. 

### <a name="new-apis"></a>Introducing New APIs

An API is a `Route` in Vertx's terminology. To add a new API, we want to define the route.

First, identify in which module does the API belong to.
All the modules are listed under [modules](server/src/main/java/org/apache/cassandra/sidecar/modules). 
For example, if the new API is to trigger an operation in Cassandra, it fits in the 
[CassandraOperationsModule](server/src/main/java/org/apache/cassandra/sidecar/modules/CassandraOperationsModule.java).
Similarly, a health check API should be located in the 
[HealthCheckModule](server/src/main/java/org/apache/cassandra/sidecar/modules/HealthCheckModule.java).

Second, declare the route to be injected into `Router`. Take the following code snippet as example,

```java
@GET
@Path(ApiEndpointsV1.HEALTH_ROUTE)
@Operation(summary = "Check Sidecar health",
           description = "Returns the health status of the Sidecar application")
@APIResponse(description = "Sidecar is healthy",
             responseCode = "200",
             content = @Content(mediaType = "application/json",
              schema = @Schema(implementation = HealthResponse.class)))
@ProvidesIntoMap
@KeyClassMapKey(VertxRouteMapKeys.SidecarHealthRouteKey.class)
VertxRoute sidecarHealthRoute(RouteBuilder.Factory factory)
{
  return factory.builderForUnprotectedRoute()
                .handler(context -> context.json(OK_STATUS))
                .build();
}
```

The method builds a `VertxRoute` using `RouteBuilder`. You can see the builder defines the `HttpMethod`,
the `endpoint` and the request handler. In addition, you may notice the annotations such as `@ProvidesIntoMap`
and `@KeyClassMapKey`. The annotations work together to inject the binding into `MapBinder`, which is eventually 
used to build the complete router. To learn more about dependency injection usage in this project, see [Guice in Sidecar](#guice) 
and [Guice Best Practices in Sidecar](server/src/main/java/org/apache/cassandra/sidecar/modules/guice-best-practice.md).

### <a name="async-programming"></a>Asynchronous Programming

In the Asynchronous Programming model, the same hardware is able to handle more requests. This model uses fewer threads
to process incoming connections. When blocking I/O operations occur, the thread moves on to the next task (handling
a different request for example), and then once the I/O operation has completed the thread will come back to the
initial task.

Vertx multiplexes concurrent workloads using event loops. This allows taking advantage of the existing hardware more
effectively to handle more requests.

Any blocking I/O or CPU intensive processing needs to be handled outside the event loop threads. NEVER BLOCK THE
EVENT LOOP THREAD!

### <a name="thread-pools"></a>Thread Pool Model

Vertx uses different thread pools for internal processing. By default, vertx will use the event loop thread pool,
the worker thread pool, and the internal worker thread pool. We also introduced `ExecutorPools` in Sidecar to run
blocking executions and can be configured separately. `ExecutorPools` should be preferred over the worker and internal
worker pools from vertx.

#### The event loop thread pool (i.e. vert.x-eventloop-thread-1)

The event loop thread pool threads handle events for processing. When the event comes in, it is dispatched to a
handler. It is expected that the processing will be complete quickly per event. If it doesn't you will see log
entries warning you that the event loop thread has been blocked. Vertx provisions a thread to detect blocked threads
in the execution.

```
Thread vertx-eventloop-thread-3 has been blocked for 20458 ms
```

If you see log entries like the one above, make sure to audit your code to understand your code and what is
causing your code to block the event pool thread. Consider moving the blocking I/O operations or CPU-intensive
operations to a worker thread pool.

#### The worker thread pool (i.e. vert.x-worker-thread-1)

This thread pool is dedicated to handling blocking I/O operations or CPU-intensive operations that might block
event loop threads. By default, the thread pool has 20 threads. The number of worker threads can be configured
when configuring the `DeploymentOptions` for vertx.

#### The internal worker thread pool (i.e. vert.x-internal-worker-thread-1)

An internal worker thread pool used by vertx for internal operations. Similarly to the worker thread pool, the
internal worker thread pool has 20 threads by default. Do not use this thread pool directly, use the worker
thread pool instead.

#### ExecutorPools

It manages worker pools to schedule and execute _blocking_ tasks on dedicated threadpools to avoid blocking
netty eventloop. It is a facade to handle one-off and periodic blocking execution.

It exposes 2 worker pools. The `service` worker pool is for short-lived tasks, which should not occupy a thread for too 
long. Typically, those tasks are client-facing, triggered by the http requests. The `internal` worker pool is for
internal background tasks. Those tasks can live longer and potentially get queued up if prior tasks take a longer
duration. In other words, they are not time sensitive.

The `service` worker pool has the name `sidecar-worker-pool`.

The `internal` worker pool has the name `sidecar-internal-worker-pool`.

### <a name="vertx-threading"></a>Vert.x Threading Model

**Critical**: Each HTTP request MUST be processed by exactly ONE Vert.x event loop thread throughout its entire lifecycle.

When integrating external async APIs (Caffeine cache, AWS SDK, CompletableFuture-based libraries), their callbacks execute on
arbitrary threads. This violates Vert.x's threading model and can cause race conditions, data corruption, and intermittent bugs.

**Pattern**: Always capture the Vert.x context at handler entry and restore it before accessing request state:

```java
public void handle(RoutingContext context) {
    Context originCtx = Vertx.currentContext();  // Capture context

    externalAsyncOperation()
    .onSuccess(result -> {
        originCtx.runOnContext(v -> {  // Restore context
            context.next();
        });
    });
}
```

For detailed examples and best practices, see the [Vert.x Threading Model Guide](docs/vertx-threading-model.md).

### <a name="timers"></a>One-shot Timers and Periodic Timers

Use vertx APIs to set one-shot timers and periodic timers. If you need to execute a one-time operation in the future,
or if you need to run periodic operations within vertx, an API is available. These timers utilize vertx provisioned
threads that are managed internal by vertx. For example, the below code snippet runs `action()` after `delayMillis`.

```java
executorPools.service().setTimer(delayMillis, timerId -> action());
```

Similarly, a simple periodic task can be scheduled with the following code. The `action()` is scheduled to run after 
the `initialDelayMillis` and repeat every `delayMillis`.

```java
executorPools.internal().setPeriodic(initialDelayMillis, delayMillis, timerId -> action());
```
Note that such periodic task is triggered whenever the scheduled time has arrived, consider the equivalent 
`ScheduledExecutorService#scheduleAtFixedRate`. It is possible to have concurrent runs, depending on the delay parameters. 
It might not be the desired scheduling behavior for the use case. If serial execution sequence is wanted, check out the 
scheduling mechanism described in [Advanced periodic task scheduling](#advanced-periodic-scheduling)

#### <a name="advanced-periodic-scheduling">Advanced Periodic Task Scheduling

`PeriodicTaskExecutor` provides the scheduling behavior that is similar to the hybrid of `ScheduledExecutorService#scheduleAtFixedRate`
and `ScheduledExecutorService#scheduleAtFixedDelay`. The unit of execution is `PeriodicTask`.

A `PeriodicTask` is guaranteed to be executed in serial by `PeriodicTaskExecutor`, as if such task is executed from 
a single thread. Memory consistency is ensured, i.e. the effect of write from the last run is visible to the current run.
The interval between the consecutive runs is adjusted based on the duration taken by the last run. This way, its behavior 
is similar to providing a "fixed rate". Note that, if the last run takes more time than the interval, the next run starts
immediately, right _after_ the completion of the prior run. 

You may refer to `org.apache.cassandra.sidecar.db.schema.SidecarSchemaInitializer` as an example of `PeriodicTask`.

### <a name="guice"></a>Guice in Sidecar

> ℹ️ [Guice Wiki](https://github.com/google/guice/wiki/)

The Apache Cassandra Sidecar project uses Guice for [dependency injection](https://en.wikipedia.org/wiki/Dependency_injection),
managing the dependency graph, promoting clean and maintainable code.

If you are new to Guice, please refer to [Guice Wiki](https://github.com/google/guice/wiki/) to get familiar with the framework.

#### Dependency Injection

Dependency injection is fundamental in Guice. Below is a short example to demonstrate how it is set up and how it works. Refer to
[Guice Wiki](https://github.com/google/guice/wiki/), if you want to see more examples.

The Guice Module, `ChecksumVerificationModule`, declares a singleton `ChecksumVerifier` implementation based on MD5. The binding
can be illustrated as such, `ChecksumVerifier -> MD5ChecksumVerifier`, i.e. whenever `ChecksumVerifier` is wanted, Guice injects 
the bound implementation, i.e. `MD5ChecksumVerifier`. In this example, `FileReceiver` is instantiated with `MD5ChecksumVerifier`.

```java
public class ChecksumVerificationModule extends AbstractModule
{
    @Provides
    @Singleton
    ChecksumVerifier md5ChecksumVerifier()
    {
        return new MD5ChecksumVerifier();
    }
    
    @Provides
    @Singleton
    FileReceiver fileReceiver(ChecksumVerifier md5Verifier)
    {
        return new FileReceiver(md5Verifier);
    }

    class MD5ChecksumVerifier implements ChecksumVerifier
    {
        @Override
        public Future<Boolean> verify(String expectedHash, String filename)
        {
            // calculate the MD5 of the file located by filename and verify with the expectedHash
        }
    }
}
```

A new implementation of `ChecksumVerifier` that uses a different hashing algorithm can be injected later. In that case, 
[@Named](https://github.com/google/guice/wiki/BindingAnnotations#named) annotation might be used to differentiate the 
implementations binding to the same interface. 

#### Guice Best Practices in Sidecar 

More Guice and its best practices in this project, please see 
[Guice Best Practice in Sidecar](server/src/main/java/org/apache/cassandra/sidecar/modules/guice-best-practice.md)

### <a name="chaining-handlers"></a>Handler Chaining

Vertx allows you to chain handlers. Each handler in the chain will process a small unit of operation. This becomes
useful when you want to reuse code in different routes. For example the `FileStreamHandler` which is used by multiple
routes.

Here's an example of a route that uses multiple handlers.


```java
router.route().method(HttpMethod.GET)
      .path("/api/v1/keyspaces/:keyspace/tables/:table/snapshots/:snapshot/components/:component")
      .handler(validateQualifiedTableHandler)
      .handler(streamSSTableComponentHandler)
      .handler(fileStreamHandler);
```

This route chains three handlers. The first handler validates the keyspace and table name provided as part of the
path parameters. The second handler determines the path on disk of the component that is going to be streamed. Lastly,
the file stream handler will stream the file from the previous handler back to the client.

Note how the validation handler can be reused by other routes that also need keyspace and table name validation;
and the filestream handler can be reused by other routes that need to perform file streaming.

Let's take a look at these handlers a little more in detail.

```java
public class ValidateQualifiedTableHandler implements Handler<RoutingContext> {
    public void handle(RoutingContext ctx) {
        String ks = ctx.pathParam("keyspace");
        String tn = ctx.pathParam("table");
        if (!isValidKeyspaceName(ks)) ctx.fail("Invalid keyspace");
        else if (!isValidTable(tn)) ctx.fail("Invalid table");
        else ctx.next();
    }
}
```

```java
public class StreamSSTableComponentHandler implements Handler<RoutingContext> {
    public void handle(RoutingContext ctx) {
        String ks = ctx.pathParam("keyspace");
        String tn = ctx.pathParam("table");
        …
        pathBuilder.build(host, ks, tn, snapshot, component)
                   .onSuccess(p -> ctx.put("filename", p).next())
                   .onFailure(ctx::fail);
    }
}
```

```java
public class FileStreamHandler implements Handler<RoutingContext>  {
    public void handle(RoutingContext context) {
        final String localFile = context.get("filename");
        FileSystem fs = context.vertx().fileSystem();
        fs.exists(localFile)
          .compose(exists -> ensureValidFile(fs, localFile, exists))
          .compose(fileProps -> fileStreamer.stream(new HttpResponse(context.response()), localFile, fileProps.size(), context.request().getHeader(HttpHeaderNames.RANGE)))
          .onFailure(context::fail);
    }
}
```

### <a name="async-handlers"></a>Asynchronous Handlers

Vertx has support for both asynchronous and blocking handlers. When using a blocking handler, you will take up a worker thread for the entire
execution of the handler. This is similar to what the [traditional application servers](#traditional-app-server)
do with their threading model, which doesn't take full advantage of the asynchronous and reactive benefits that vertx
offers.

Blocking handler:

```java
router.get("/blockingHandler")
      .blockingHandler(listSnapshotFiles);
```

Asynchronous handlers (Preferred):

```java
router.route().method(HttpMethod.GET)
      .path("/asyncHandler")
      .handler(streamSSTableComponentHandler)
      .handler(fileStreamHandler);
```

The blocking actions as part of `/asyncHandler` can be dispatched to `ExecutorPool` to be async. Therefore,
unless the blocking handler is very simple, asynchronous handler should be preferred.

### <a name="future-composition"></a>Future Composition

Future composition is a feature that allows you to chain multiple futures. When the current future succeeds, then
it applies to the function down the chain. When one of the futures fails, the composition fails.

```java
FileSystem fs = vertx.fileSystem();
Future<Void> future = fs.createFile("/foo")
                        .compose(v -> fs.writeFile("/foo", Buffer.buffer()))
                        .compose(v -> fs.move("/foo", "/bar"));
```

If one of the future fails above, the chain is stopped and a failed future results from the chain. If all the futures
succeed, then the chain succeeds.

### <a name="failure-handling"></a>Failure Handling

If you need to provide feedback to the client about a request, use `HttpException` with the appropriate status code
and a message that describes the issue and helps the client fix the issue.

For example:

```java
throw new HttpException(BAD_REQUEST.code(), "Computed MD5 checksum does not match expected");
```

The exception can be added directly to the `RequestContext` inside the handler:

```java
context.fail(new HttpException(TOO_MANY_REQUESTS.code(), "Retry exhausted"));
```

Be careful what you add in the response payload of the `HttpException`. Bad actors can use information from these
responses to try to compromise the system.

A look at the new `SidecarFailureHandler` class:

```java
public class SidecarFailureHandler implements Handler<RoutingContext> {
  @Override
  public void handle(RoutingContext ctx) {
    Throwable t = ctx.failure();
    if (t instanceOf HttpException) handleHttpException(ctx, (HttpException) t);
    else if (ctx.statusCode() == REQUEST_TIMEOUT.code())
      handleRequestTimeout(ctx);
    else ctx.next(); // handled by vertx
  }

  private void handleHttpException(RoutingContext ctx, HttpException e) {
      JsonObject payload = new JsonObject()
                           .put("status", "Fail")
                           .put("message", e.getPayload());
      writeResponse(ctx, e.getStatusCode(), payload);
  }
  …
}
```

### <a name="cassandra-adapters"></a>Cassandra Adapters
The Cassandra Sidecar is designed to support multiple Cassandra versions, including multiple, different instances on the same host.
The `adapters` subproject contains an implementation of the Cassandra adapter for different versions of Cassandra.
The `base` adapter supports Cassandra 4.0 and greater, including `trunk`.
When some form of breaking change is necessary in `base`, the new functionality should be developed in the `base` adapter,
and then any shim necessary to work with the older version(s) should be added to a new adapter package.
The `CassandraAdapterFactory` has a version tag which represents the minimum version that particular adapter package supports.
When adding shims, implement the minimum necessary changes in the new package and name all classes with a version number after the word `Cassandra`.
For example, if `base`'s minimum version is moved to 5.0, a Cassandra40 adapter package/subproject should be added, with a minimum version of 4.0.0.
Within that project, the classes should all be named `Cassandra40*`, so `Cassandra40Adapter`, `Cassandra40Factory`, etc.
