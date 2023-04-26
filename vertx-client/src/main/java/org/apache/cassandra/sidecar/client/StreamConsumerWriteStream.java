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

package org.apache.cassandra.sidecar.client;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;

/**
 * A {@link WriteStream} that writes available data to the {@link StreamConsumer}
 */
public class StreamConsumerWriteStream implements WriteStream<Buffer>
{
    private final StreamConsumer streamConsumer;

    /**
     * Constructs a WriteStream with the provided {@code streamConsumer}
     *
     * @param streamConsumer the consumer of the data being streamed
     */
    public StreamConsumerWriteStream(StreamConsumer streamConsumer)
    {
        this.streamConsumer = streamConsumer;
    }

    @Override
    public WriteStream<Buffer> exceptionHandler(Handler<Throwable> handler)
    {
        return this;
    }

    @Override
    public Future<Void> write(Buffer data)
    {
        streamConsumer.onRead(new VertxStreamBuffer(data));
        return Future.succeededFuture();
    }

    @Override
    public void write(Buffer data, Handler<AsyncResult<Void>> handler)
    {
        streamConsumer.onRead(new VertxStreamBuffer(data));
        if (handler != null)
        {
            handler.handle(Future.succeededFuture());
        }
    }

    @Override
    public void end(Handler<AsyncResult<Void>> handler)
    {
        streamConsumer.onComplete();
        if (handler != null)
        {
            handler.handle(Future.succeededFuture());
        }
    }

    @Override
    public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize)
    {
        return this;
    }

    @Override
    public boolean writeQueueFull()
    {
        return false;
    }

    @Override
    public WriteStream<Buffer> drainHandler(Handler<Void> handler)
    {
        return this;
    }
}
