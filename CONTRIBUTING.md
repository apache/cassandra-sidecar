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
* [Source Code Best Practices](#best-practices)
  * [Asynchronous Programming](#async-programming)
  * [Thread Pool Model](#thread-pools)
  * [One-shot Timers and Periodic Timers](#timers)
  * [RestEasy Integration](#resteasy)
  * [Dependency Injection](#guice)
  * [Handler Chaining](#chaining-handlers)
  * [Asynchronous Handlers](#async-handlers)
  * [Future Composition](#future-composition)
  * [Failure Handling](#failure-handling)
  * [Cassandra Adapters](#cassandra-adapters)
* [Source Code Style](#source-code-style)

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

## <a name="best-practices"></a>Source Code Best Practices

The Apache Cassandra Sidecar project uses the [vertx](https://vertx.io) toolkit. It uses the asynchronous and
reactive programming paradigm. This allows for Sidecar to scale up as workloads grow, as well as resiliency when
failures arise.

<a name="traditional-app-server"></a>
In a traditional application server, a pool of threads is used to receive request. Each request is handled by a
distinct thread. This model is well suited for small-medium workloads. As workloads grow, applications need to scale
horizontally because of hardware limits on the number of threads that can be created.

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

### <a href="timers"></a>One-shot Timers and Periodic Timers

Use vertx APIs to set one-shot timers and periodic timers. If you need to execute a one-time operation in the future,
or if you need to run periodic operations within vertx, an API is available. These timers utilize vertx provisioned
threads that are managed internal by vertx. For example

```java
logger.debug("Retrying streaming after {} millis", millis);
executorPools.service().setTimer(millis, t -> acquireAndSend(context, filename, fileLength, range, startTime));
```

### <a href="resteasy"></a>RestEasy Integration

The Apache Cassandra Sidecar project uses vertx-resteasy, and is the preferred method to provision endpoints. The
exception to the rule is when you require access to low-level APIs that are not available through RestEasy. Here's
an example of a simple RestEasy handler.

```java
@Path("/api/v1/time-skew")
public class TimeSkewHandler {
  private final TimeSkewInfo info;

  @Inject
  public TimeSkewHandler(TimeSkewInfo info) {
    info = info;
  }

  @GET
  public TimeSkewResponse getTimeSkewResponse() {
    return new TimeSkewResponse(System.currentTimeMillis(), info.allowableSkewInMinutes());;
  }
}
```

### <a href="guice"></a>Dependency Injection

The Apache Cassandra Sidecar project uses Guice for handling the object interdependency. When a class encapsulates
some functionality that is then used by a different class in the system, we use Guice for dependency injection.

When a different implementation can be provided in the future, prefer using an interface and a default implementation
that can be later switched transparently without affecting the classes that depend on it.

Prefer creating concrete classes over utility methods. The concrete classes can be managed as a
[Singleton](https://en.wikipedia.org/wiki/Singleton_pattern) if they do not have external dependencies that might
change their behavior based on the configuration.

Here's an example of a class being managed by Guice.

```java
/**
 * Verifies the checksum of a file
 */
public interface ChecksumVerifier {
  Future<Boolean> verify(String expectedhash, String filename);
}
```

Let's say we support `MD5` for checksum verification, our implementation can look like this:

```java
public class MD5ChecksumVerifier implements ChecksumVerifier {
    public Future<Boolean> verify(String expectedhash, String filename) {
        return fs.open(filename, new OpenOptions())
                 .compose(this::calculateMD5)
                 .compose(computedChecksum -> {
                     if (!expectedHash.equals(computedChecksum))
                         return Future.failedFuture();
                     return Future.succeededFuture(true);
                 });
    }
  }
```

A new implementation of `ChecksumVerifier` that uses a different hashing algorithm can be injected later.

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

When [RestEasy](#resteasy) is not a suitable approach, use asynchronous handlers. Vertx has support for both
asynchronous and blocking handlers. When using a blocking handler, you will take up a worker thread for the entire
execution of the handler. This is similar to what the [traditional application servers](#traditional-app-server)
do with their threading model, which doesn't take full advantage of the asynchronous and reactive benefits that vertx
offers.

Blocking handler:

```java
router.get("/blockingHandler")
      .blockingHandler(listSnapshotFiles);
```

Asynchronous handlers:

```java
router.route().method(HttpMethod.GET)
      .path("/asyncHandler")
      .handler(streamSSTableComponentHandler)
      .handler(fileStreamHandler);
```

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

### <a name="cassandra-adapters></a>Cassandra Adapters
The Cassandra Sidecar is designed to support multiple Cassandra versions, including multiple, different instances on the same host.
The `adapters` subproject contains an implementation of the Cassandra adapter for different versions of Cassandra.
The `base` adapter supports Cassandra 4.0 and greater, including `trunk`.
When some form of breaking change is necessary in `base`, the new functionality should be developed in the `base` adapter,
and then any shim necessary to work with the older version(s) should be added to a new adapter package.
The `CassandraAdapterFactory` has a version tag which represents the minimum version that particular adapter package supports.
When adding shims, implement the minimum necessary changes in the new package and name all classes with a version number after the word `Cassandra`.
For example, if `base`'s minimum version is moved to 5.0, a Cassandra40 adapter package/subproject should be added, with a minimum version of 4.0.0.
Within that project, the classes should all be named `Cassandra40*`, so `Cassandra40Adapter`, `Cassandra40Factory`, etc.

## <a name="source-code-style"></a>Source Code Style

The project provides an
[IntelliJ IDEA code formatting configuration](https://github.com/apache/cassandra-sidecar/blob/trunk/ide/idea/codeStyleSettings.xml)
that defines the source file coding standards.

To import the formatting configuration run the following gradle task:

```shell
./gradlew idea
```

This will install the style settings into the `.idea` directory located at the root of the project directory.
You can then use the provided configuration that adheres to the project source file coding standards.
