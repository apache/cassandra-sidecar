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

import java.io.Closeable;

/**
 * Abstraction of hash algorithm
 */
public interface Hasher extends Closeable
{
    /**
     * Updates the value of the hash with buf[off:off+len].
     *
     * @param buf the input data
     * @param off the start offset in buf
     * @param len the number of bytes to hash
     */
    void update(byte[] buf, int off, int len);

    /**
     * Returns the value of the checksum.
     *
     * @return the checksum
     */
    String checksum();
}
