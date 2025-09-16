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

package org.apache.cassandra.sidecar.utils;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteStreams;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.fail;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@link ByteBufUtils}
 */
class ByteBufUtilsTest
{
    @Test
    void testSkipBytesFully() throws IOException
    {
        testSkipBytesFully("abc".getBytes(StandardCharsets.UTF_8));
        testSkipBytesFully("abcdefghijklm".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void testReadRemainingBytes() throws IOException
    {
        testReadRemainingBytes("");
        testReadRemainingBytes("abc");
        testReadRemainingBytes("abcdefghijklm");
    }

    @Test
    void testGetArray()
    {
        testGetArray("");
        testGetArray("abc");
        testGetArray("abcdefghijklm");
    }

    @Test
    void testHexString()
    {
        assertThat(ByteBufUtils.toHexString(ByteBuffer.allocate(8).putLong(500L).flip())).isEqualTo("00000000000001F4");
        assertThat(ByteBufUtils.toHexString(ByteBuffer.wrap(new byte[]{ 'a', 'b', 'c' }))).isEqualTo("616263");
        assertThat(ByteBufUtils.toHexString(ByteBuffer.allocate(8).putLong(92848484L).asReadOnlyBuffer().flip())).isEqualTo("000000000588C164");
        assertThat(ByteBufUtils.toHexString(null)).isEqualTo("null");

        assertThat(ByteBufUtils.toHexString(new byte[]{ 'a', 'b', 'c' }, 0, 3)).isEqualTo("616263");
        assertThat(ByteBufUtils.toHexString(new byte[]{ 'a', 'b', 'c' }, 2, 1)).isEqualTo("63");
    }

    private static void testGetArray(String str)
    {
        assertThat(new String(ByteBufUtils.getArray(ByteBuffer.wrap(str.getBytes())), StandardCharsets.UTF_8)).isEqualTo(str);
    }

    private static void testReadRemainingBytes(String str) throws IOException
    {
        assertThat(new String(ByteBufUtils.readRemainingBytes(new ByteArrayInputStream(str.getBytes()), str.length()), StandardCharsets.UTF_8)).isEqualTo(str);
    }

    private static void testSkipBytesFully(byte[] ar) throws IOException
    {
        int len = ar.length;
        ByteArrayDataInput in = ByteStreams.newDataInput(ar, 0);
        ByteBufUtils.skipBytesFully(in, 1);
        ByteBufUtils.skipBytesFully(in, len - 2);
        assertThat(in.readByte()).isEqualTo(ar[len - 1]);
        try
        {
            ByteBufUtils.skipBytesFully(in, 1);
            fail("EOFException should have been thrown");
        }
        catch (EOFException ignore)
        {
        }
    }
}
