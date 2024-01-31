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

package org.apache.cassandra.sidecar.common.utils;

/**
 * A utility class to perform byte operations
 */
public final class ByteUtils
{
    /**
     * {@code 1 Kibibyte} is equivalent to {@code 1,024 bytes}
     */
    public static final long ONE_KIB = 1L << 10;

    /**
     * {@code 1 Mebibyte} is equivalent to {@code 1,048,576 bytes}
     */
    public static final long ONE_MIB = ONE_KIB << 10;

    /**
     * {@code 1 Gibibyte} is equivalent to {@code 1,073,741,824 bytes}
     */
    public static final long ONE_GIB = ONE_MIB << 10;

    /**
     * {@code 1 Tebibyte} is equivalent to {@code 1,099,511,627,776 bytes}
     */
    public static final long ONE_TIB = ONE_GIB << 10;

    /**
     * {@code 1 Pebibyte} is equivalent to {@code 1,125,899,906,842,624 bytes}
     */
    public static final long ONE_PIB = ONE_TIB << 10;

    /**
     * {@code 1 Exbibyte} is equivalent to {@code 1,152,921,504,606,846,976 bytes}
     */
    public static final long ONE_EIB = ONE_PIB << 10;

    /**
     * Returns a human-readable representation of the number of {@code bytes} in the binary prefix format. A
     * long input can only represent up to exbibytes units.
     *
     * @param bytes the non-negative number of bytes
     * @return a human-readable representation of the number of {@code bytes} in the binary prefix format
     * @throws IllegalArgumentException when {@code bytes} is a negative value
     */
    public static String bytesToHumanReadableBinaryPrefix(long bytes)
    {
        Preconditions.checkArgument(bytes >= 0, "bytes cannot be negative");

        if (bytes >= ONE_EIB) return formatHelper(bytes, ONE_EIB, "EiB");
        if (bytes >= ONE_PIB) return formatHelper(bytes, ONE_PIB, "PiB");
        if (bytes >= ONE_TIB) return formatHelper(bytes, ONE_TIB, "TiB");
        if (bytes >= ONE_GIB) return formatHelper(bytes, ONE_GIB, "GiB");
        if (bytes >= ONE_MIB) return formatHelper(bytes, ONE_MIB, "MiB");
        if (bytes >= ONE_KIB) return formatHelper(bytes, ONE_KIB, "KiB");
        return bytes + " B";
    }

    static String formatHelper(double bytes, long baseUnit, String unitName)
    {
        return String.format("%.2f %s", bytes / baseUnit, unitName);
    }
}
