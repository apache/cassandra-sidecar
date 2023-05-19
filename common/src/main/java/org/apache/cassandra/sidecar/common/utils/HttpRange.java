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

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.sidecar.common.exceptions.RangeException;
import org.jetbrains.annotations.NotNull;

/**
 * Accepted Range formats are start-end, start-, -suffix_length
 * start-end (start = start index of the range, end = end index of the range, both inclusive)
 * start- (start = start index of the range, end = end of file)
 * -suffix-length (Requested length from end of file. The length should be positive)
 */
public class HttpRange
{
    private static final String RANGE_UNIT = "bytes";
    // matches a. bytes=1-2, b. bytes=1-, c. bytes=-2, d. bytes=-. Need to do another valid for case d.
    private static final Pattern RANGE_HEADER = Pattern.compile("^" + RANGE_UNIT + "=(\\d*)-(\\d*)$");
    private static final long BOUND_ABSENT = -1L;
    private final long start;
    private final long end;
    private final long length;

    // An initialized range is always valid; invalid params fail range initialization.
    private HttpRange(final long start, final long end)
    {
        Preconditions.checkArgument(start >= 0, "Range start can not be negative");
        Preconditions.checkArgument(end >= start, "Range does not satisfy boundary requirements");
        this.start = start;
        this.end = end;
        long len = end - start + 1; // Assign long max if overflows
        this.length = len < 0 ? Long.MAX_VALUE : len;
    }

    /**
     * Accepted RangeHeader formats are bytes=start-end, bytes=start-, bytes=-suffix_length
     *
     * @param header   the header string to be parsed
     * @param fileSize the size of the file
     * @return the parsed HTTP range header
     */
    public static HttpRange parseHeader(final String header, final long fileSize)
    {
        if (header == null)
        {
            return new HttpRange(0, fileSize - 1);
        }
        return HttpRange.parse(header, fileSize);
    }

    public static HttpRange of(final long start, final long end)
    {
        return new HttpRange(start, end);
    }

    /**
     * Accepted string formats "bytes=1453-3563", "bytes=-22344", "bytes=5346-"
     * Sample invalid string formats "bytes=8-3", "bytes=-", "bytes=-0", "bytes=a-b"
     *
     * @param fileSize - passed in to convert partial range into absolute range
     */
    private static HttpRange parse(@NotNull String rangeHeader, final long fileSize)
    {
        Matcher m = RANGE_HEADER.matcher(rangeHeader);
        if (!m.matches())
        {
            throw invalidRangeHeaderException(rangeHeader);
        }

        long left = parseLong(m.group(1), rangeHeader);
        long right = parseLong(m.group(2), rangeHeader);

        if (left == BOUND_ABSENT && right == BOUND_ABSENT) // matching "bytes=-"
        {
            throw invalidRangeHeaderException(rangeHeader);
        }
        else if (left == BOUND_ABSENT) // matching "bytes=-1"
        {
            long len = Math.min(right, fileSize); // correct the range if it exceeds file size.
            return new HttpRange(fileSize - len, fileSize - 1);
        }
        else if (right == BOUND_ABSENT) // matching "bytes=1-"
        {
            return new HttpRange(left, fileSize - 1);
        }
        else
        {
            return new HttpRange(left, right);
        }
    }

    // return -1 for empty string; return long value otherwise.
    // throws IllegalArgumentException for invalid value string
    private static long parseLong(String valStr, String rangeHeader)
    {
        if (valStr == null || valStr.isEmpty())
            return BOUND_ABSENT;

        try
        {
            return Long.parseLong(valStr);
        }
        catch (NumberFormatException e)
        {
            throw invalidRangeHeaderException(rangeHeader);
        }
    }

    private static IllegalArgumentException invalidRangeHeaderException(String rangeHeader)
    {
        return new IllegalArgumentException("Invalid range header: " + rangeHeader + ". " +
                                            "Supported Range formats are bytes=<start>-<end>, " +
                                            "bytes=<start>-, bytes=-<suffix-length>");
    }

    public long start()
    {
        return this.start;
    }

    public long end()
    {
        return this.end;
    }

    public long length()
    {
        return this.length;
    }

    public HttpRange intersect(@NotNull final HttpRange range)
    {
        long newStart = Math.max(start, range.start());
        long newEnd = Math.min(end, range.end());
        if (newStart > newEnd)
        {
            throw new RangeException("Range does not overlap");
        }

        return new HttpRange(newStart, newEnd);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof HttpRange))
        {
            return false;
        }
        HttpRange range = (HttpRange) o;
        return start == range.start &&
               end == range.end &&
               length == range.length;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(start, end, length);
    }

    @Override
    public String toString()
    {
        return String.format("%s=%d-%d", RANGE_UNIT, start, end);
    }
}
