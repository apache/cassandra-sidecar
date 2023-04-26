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

import java.nio.ByteBuffer;

import io.vertx.core.buffer.Buffer;

/**
 * A {@link StreamBuffer} implementation that wraps a vertx {@link Buffer}
 */
public class VertxStreamBuffer implements StreamBuffer
{
    private final Buffer buffer;

    /**
     * Constructs a new instance with the provide {@code buffer}
     *
     * @param buffer the source buffer for the data
     */
    public VertxStreamBuffer(Buffer buffer)
    {
        this.buffer = buffer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyBytes(int sourceOffset, ByteBuffer destination, int length)
    {
        destination.put(buffer.getBytes(sourceOffset, sourceOffset + length));
        destination.flip();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyBytes(int sourceOffset, byte[] destination, int destinationIndex, int length)
    {
        buffer.getBytes(sourceOffset, sourceOffset + length, destination, destinationIndex);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte getByte(int index)
    {
        return buffer.getByte(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int readableBytes()
    {
        return buffer.length();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void release()
    {
        // DO NOTHING
    }
}
