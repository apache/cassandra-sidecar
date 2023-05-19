/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.client;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link StreamBuffer}
 */
class StreamBufferTest
{
    static byte[] source;
    static StreamBuffer.ByteArrayWrapper streamBuffer;

    @BeforeAll
    static void setup()
    {
        source = new byte[1024];
        source[5] = 'a';
        streamBuffer = StreamBuffer.wrap(source);
    }

    @Test
    void testStreamBuffer()
    {
        assertThat(streamBuffer).isNotNull();
        assertThat(streamBuffer.readableBytes()).isEqualTo(1024);
        assertThat(streamBuffer.getByte(5)).isEqualTo((byte) 'a');
        streamBuffer.release(); // do nothing
    }

    @Test
    void testCopyToByteArray()
    {
        byte[] dst = new byte[1];
        streamBuffer.copyBytes(5, dst, 0, 1);
        assertThat(dst[0]).isEqualTo((byte) 'a');
    }

    @Test
    void testFullCopyBytesToByteBuffer()
    {
        StreamBuffer streamBuffer = StreamBuffer.wrap("Hello World".getBytes(StandardCharsets.UTF_8));
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
        StreamBuffer streamBuffer = StreamBuffer.wrap("Hello World".getBytes(StandardCharsets.UTF_8));
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
    void testGetByte()
    {
        String inputString = "take me, one at a time";
        StreamBuffer streamBuffer = StreamBuffer.wrap(inputString.getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < inputString.length(); i++)
        {
            assertThat(streamBuffer.getByte(i)).isEqualTo((byte) inputString.charAt(i));
        }
    }
}
