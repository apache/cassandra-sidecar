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

package org.apache.cassandra.sidecar.utils;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.Supplier;

/**
 * Utility class providing byte buffer manipulation and conversion operations for Cassandra Sidecar.
 */
public class ByteBufUtils
{
    public static final ThreadLocal<CharsetDecoder> UTF8_DECODER_PROVIDER = ThreadLocal.withInitial(StandardCharsets.UTF_8::newDecoder);
    public static final String EMPTY_STR = "";
    private static final int STATIC_MARKER = 0xFFFF;
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    public static void skipBytesFully(DataInput in, int bytes) throws IOException
    {
        int n = 0;
        while (n < bytes)
        {
            int skipped = in.skipBytes(bytes - n);
            if (skipped == 0)
            {
                throw new EOFException("EOF after " + n + " bytes out of " + bytes);
            }
            n += skipped;
        }
    }

    public static byte[] readRemainingBytes(InputStream in, int size) throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream(size);
        byte[] ar = new byte[size];
        int len;
        while ((len = in.read(ar)) != -1)
        {
            out.write(ar, 0, len);
        }
        return out.toByteArray();
    }

    public static byte[] getArray(ByteBuffer buffer)
    {
        int length = buffer.remaining();

        if (buffer.hasArray())
        {
            int boff = buffer.arrayOffset() + buffer.position();
            return Arrays.copyOfRange(buffer.array(), boff, boff + length);
        }
        // else, DirectByteBuffer.get() is the fastest route
        byte[] bytes = new byte[length];
        buffer.duplicate().get(bytes);

        return bytes;
    }

    private static String toHexString(byte[] bytes, int length)
    {
        return toHexString(bytes, 0, length);
    }

    static String toHexString(byte[] bytes, int offset, int length)
    {
        char[] hexCharacters = new char[length << 1];

        int decimalValue;
        for (int i = offset; i < offset + length; i++)
        {
            // Calculate the int value represented by the byte
            decimalValue = bytes[i] & 0xFF;
            // Retrieve hex character for 4 upper bits
            hexCharacters[(i - offset) << 1] = HEX_ARRAY[decimalValue >> 4];
            // Retrieve hex character for 4 lower bits
            hexCharacters[((i - offset) << 1) + 1] = HEX_ARRAY[decimalValue & 0xF];
        }

        return new String(hexCharacters);
    }

    public static String toHexString(ByteBuffer buffer)
    {
        if (buffer == null)
        {
            return "null";
        }

        if (buffer.isReadOnly())
        {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.slice().get(bytes);
            return ByteBufUtils.toHexString(bytes, bytes.length);
        }

        return ByteBufUtils.toHexString(buffer.array(),
                                        buffer.arrayOffset() + buffer.position(),
                                        buffer.remaining());
    }

    public static int readFully(InputStream in, byte[] b, int len) throws IOException
    {
        if (len < 0)
        {
            throw new IndexOutOfBoundsException();
        }

        int n = 0;
        while (n < len)
        {
            int count = in.read(b, n, len - n);
            if (count < 0)
            {
                break;
            }
            n += count;
        }

        return n;
    }

    // changes bb position
    public static ByteBuffer readBytesWithShortLength(ByteBuffer bb)
    {
        return readBytes(bb, readShortLength(bb));
    }

    // changes bb position
    static void writeShortLength(ByteBuffer bb, int length)
    {
        bb.put((byte) ((length >> 8) & 0xFF));
        bb.put((byte) (length & 0xFF));
    }

    // Doesn't change bb position
    static int peekShortLength(ByteBuffer bb, int position)
    {
        int length = (bb.get(position) & 0xFF) << 8;
        return length | (bb.get(position + 1) & 0xFF);
    }

    // changes bb position
    static int readShortLength(ByteBuffer bb)
    {
        int length = (bb.get() & 0xFF) << 8;
        return length | (bb.get() & 0xFF);
    }

    // changes bb position
    @SuppressWarnings("RedundantCast")
    public static ByteBuffer readBytes(ByteBuffer bb, int length)
    {
        ByteBuffer copy = bb.duplicate();
        ((Buffer) copy).limit(copy.position() + length);
        ((Buffer) bb).position(bb.position() + length);
        return copy;
    }

    public static void skipFully(InputStream is, long len) throws IOException
    {
        long skipped = is.skip(len);
        if (skipped != len)
        {
            throw new EOFException("EOF after " + skipped + " bytes out of " + len);
        }
    }

    public static ByteBuffer[] split(ByteBuffer name, int numKeys)
    {
        // Assume all components, we'll trunk the array afterwards if need be, but
        // most names will be complete.
        ByteBuffer[] l = new ByteBuffer[numKeys];
        ByteBuffer bb = name.duplicate();
        ByteBufUtils.readStatic(bb);
        int i = 0;
        while (bb.remaining() > 0)
        {
            l[i++] = readBytesWithShortLength(bb);
            bb.get(); // skip end-of-component
        }
        return i == l.length ? l : Arrays.copyOfRange(l, 0, i);
    }

    public static void readStatic(ByteBuffer bb)
    {
        if (bb.remaining() < 2)
        {
            return;
        }

        int header = peekShortLength(bb, bb.position());
        if ((header & 0xFFFF) != ByteBufUtils.STATIC_MARKER)
        {
            return;
        }

        readShortLength(bb); // Skip header
    }

    public static ByteBuffer build(boolean isStatic, ByteBuffer... buffers)
    {
        int totalLength = isStatic ? 2 : 0;
        for (ByteBuffer bb : buffers)
        {
            // 2 bytes short length + data length + 1 byte for end-of-component marker
            totalLength += 2 + bb.remaining() + 1;
        }

        ByteBuffer out = ByteBuffer.allocate(totalLength);
        if (isStatic)
        {
            out.putShort((short) STATIC_MARKER);
        }

        for (ByteBuffer bb : buffers)
        {
            writeShortLength(out, bb.remaining()); // short len
            out.put(bb.duplicate()); // data
            out.put((byte) 0); // end-of-component marker
        }
        out.flip();
        return out;
    }

    /**
     * Decode ByteBuffer into String using provided CharsetDecoder.
     *
     * @param buffer          byte buffer
     * @param decoderSupplier let the user provide their own CharsetDecoder provider 
     *                        e.g. using io.netty.util.concurrent.FastThreadLocal over java.lang.ThreadLocal
     * @return decoded string
     * @throws CharacterCodingException charset decoding exception
     */
    public static String string(ByteBuffer buffer, Supplier<CharsetDecoder> decoderSupplier) throws CharacterCodingException
    {
        if (buffer.remaining() <= 0)
        {
            return EMPTY_STR;
        }
        return decoderSupplier.get().decode(buffer.duplicate()).toString();
    }

    public static String string(ByteBuffer buffer) throws CharacterCodingException
    {
        return string(buffer, UTF8_DECODER_PROVIDER::get);
    }
}
