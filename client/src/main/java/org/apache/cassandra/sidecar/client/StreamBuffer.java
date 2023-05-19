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

/**
 * A generic wrapper around bytes to allow for on/off-heap byte arrays.
 */
public interface StreamBuffer
{
    /**
     * Copies bytes from this {@link StreamBuffer} into the {@link ByteBuffer destination}
     *
     * @param sourceOffset the offset within the {@link StreamBuffer} to be read; must be non-negative and
     *                     larger than the buffer length
     * @param destination  a {@link ByteBuffer} where the data will be copied
     * @param length       the number of bytes to be copied from the {@link StreamBuffer}; must be non-negative and
     *                     larger than the {@code buffer.length - sourceOffset}
     */
    void copyBytes(int sourceOffset, ByteBuffer destination, int length);

    /**
     * Copies bytes from this {@link StreamBuffer} into the byte array {@code destination}.
     *
     * @param sourceOffset     the offset within the {@link StreamBuffer} to be read; must be non-negative and
     *                         larger than the buffer length
     * @param destination      the target byte array where the source bytes will be copied
     * @param destinationIndex the offset in the {@code destination} where to start writing data
     * @param length           the number of bytes to be copied from the {@link StreamBuffer}; must be non-negative and
     *                         larger than the {@code buffer.length - sourceOffset}
     */
    void copyBytes(int sourceOffset, byte[] destination, int destinationIndex, int length);

    /**
     * Returns a byte from the given {@code index}.
     *
     * @param index the index in the array; must be non-negative and within the bounds of the {@link StreamBuffer}
     * @return a byte from the given {@code index}
     */
    byte getByte(int index);

    /**
     * @return the number of readable bytes from this {@link StreamBuffer}
     */
    int readableBytes();

    /**
     * Release the underlying buffer
     */
    void release();

    /**
     * Wraps a byte array into a {@link ByteArrayWrapper}
     *
     * @param bytes the underlying byte array
     * @return the object wrapping byte array
     */
    static ByteArrayWrapper wrap(byte[] bytes)
    {
        return new ByteArrayWrapper(bytes);
    }

    /**
     * A {@link StreamBuffer} implementation that wraps a byte array
     */
    class ByteArrayWrapper implements StreamBuffer
    {
        private final byte[] bytes;

        private ByteArrayWrapper(byte[] bytes)
        {
            this.bytes = bytes;
        }

        @Override
        public void copyBytes(int sourceOffset, ByteBuffer destination, int length)
        {
            destination.put(bytes, sourceOffset, length);
            destination.flip();
        }

        @Override
        public void copyBytes(int sourceOffset, byte[] destination, int destinationIndex, int length)
        {
            System.arraycopy(bytes, sourceOffset, destination, destinationIndex, length);
        }

        @Override
        public byte getByte(int index)
        {
            return bytes[index];
        }

        @Override
        public int readableBytes()
        {
            return bytes.length;
        }

        @Override
        public void release()
        {
        }
    }
}
