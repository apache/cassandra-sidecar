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
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.buffer.impl.BufferImpl;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the {@link VertxStreamBuffer} class
 */
class VertxStreamBufferTest
{
    @Test
    void testFullCopyBytesToByteBuffer()
    {
        Buffer buffer = new BufferImpl().appendString("Hello World");
        StreamBuffer streamBuffer = new VertxStreamBuffer(buffer);
        ByteBuffer destination = ByteBuffer.allocate(20);
        streamBuffer.copyBytes(0, destination, 11);
        assertThat(destination.hasArray()).isTrue();
        assertThat(destination.position()).isEqualTo(0);
        assertThat(destination.limit()).isEqualTo(11);
        byte[] dst = new byte[destination.limit()];
        destination.get(dst, 0, dst.length);
        assertThat(new String(dst, StandardCharsets.UTF_8)).isEqualTo("Hello World");
    }

    @Test
    void testPartialCopyBytesToByteBuffer()
    {
        Buffer buffer = new BufferImpl().appendString("Hello World");
        StreamBuffer streamBuffer = new VertxStreamBuffer(buffer);
        ByteBuffer destination = ByteBuffer.allocate(20);
        streamBuffer.copyBytes(0, destination, 5);
        assertThat(destination.hasArray()).isTrue();
        assertThat(destination.position()).isEqualTo(0);
        assertThat(destination.limit()).isEqualTo(5);
        byte[] dst = new byte[destination.limit()];
        destination.get(dst, 0, dst.length);
        assertThat(new String(dst, StandardCharsets.UTF_8)).isEqualTo("Hello");
    }

    @Test
    void testCopyBytesToByteArray()
    {
        Buffer buffer = new BufferImpl().appendString("Hello World");
        StreamBuffer streamBuffer = new VertxStreamBuffer(buffer);
        byte[] dest = new byte[5];
        streamBuffer.copyBytes(6, dest, 0, dest.length);
        assertThat(new String(dest, StandardCharsets.UTF_8)).isEqualTo("World");
    }

    @Test
    void testGetByte()
    {
        String inputString = "take me, one at a time";
        Buffer buffer = new BufferImpl().appendString(inputString);
        StreamBuffer streamBuffer = new VertxStreamBuffer(buffer);
        for (int i = 0; i < inputString.length(); i++)
        {
            assertThat(streamBuffer.getByte(i)).isEqualTo((byte) inputString.charAt(i));
        }
    }

    @Test
    void testReadableBytes()
    {
        Buffer buffer = new BufferImpl().appendString("Read me!");
        StreamBuffer streamBuffer = new VertxStreamBuffer(buffer);
        assertThat(streamBuffer.readableBytes()).isEqualTo(8);
        streamBuffer.release(); // does nothing
    }
}
