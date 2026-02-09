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
        assertThat(destination.position()).isEqualTo(11);
        assertThat(destination.limit()).isEqualTo(20);
        destination.flip();
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
        assertThat(destination.position()).isEqualTo(5);
        assertThat(destination.limit()).isEqualTo(20);
        destination.flip();
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

    @Test
    void testMultipleChunkCopyToSameBuffer()
    {
        // Simulates BufferingInputStream filling a buffer with multiple chunks
        // This validates the fix for the flip() bug where chunks were overwriting instead of appending

        // Create source data with 4 distinct chunks: "AAAA", "BBBB", "CCCC", "DDDD"
        String sourceData = "AAAABBBBCCCCDDDD";
        StreamBuffer streamBuffer = StreamBuffer.wrap(sourceData.getBytes(StandardCharsets.UTF_8));

        // Destination buffer to accumulate all chunks (16 bytes total)
        ByteBuffer destination = ByteBuffer.allocate(16);

        // Write chunks one at a time (simulating multi-chunk read)
        streamBuffer.copyBytes(0, destination, 4);   // Write "AAAA" at position 0
        assertThat(destination.position()).isEqualTo(4);  // Position should advance to 4

        streamBuffer.copyBytes(4, destination, 4);   // Write "BBBB" at position 4
        assertThat(destination.position()).isEqualTo(8);  // Position should advance to 8

        streamBuffer.copyBytes(8, destination, 4);   // Write "CCCC" at position 8
        assertThat(destination.position()).isEqualTo(12); // Position should advance to 12

        streamBuffer.copyBytes(12, destination, 4);  // Write "DDDD" at position 12
        assertThat(destination.position()).isEqualTo(16); // Position should advance to 16

        // Buffer should be completely filled
        assertThat(destination.remaining()).isEqualTo(0);

        // Flip to read mode and verify all chunks are present (not overwritten)
        destination.flip();
        byte[] result = new byte[16];
        destination.get(result);

        String resultString = new String(result, StandardCharsets.UTF_8);
        assertThat(resultString).isEqualTo("AAAABBBBCCCCDDDD");

        // Verify each chunk individually
        assertThat(resultString.substring(0, 4)).isEqualTo("AAAA");
        assertThat(resultString.substring(4, 8)).isEqualTo("BBBB");
        assertThat(resultString.substring(8, 12)).isEqualTo("CCCC");
        assertThat(resultString.substring(12, 16)).isEqualTo("DDDD");
    }
}
