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

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.common.exceptions.RangeException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * RangeTest
 */
public class HttpRangeTest
{
    @Test
    public void testValidPartialRange()
    {
        String rangeHeaderVal = "bytes=2-";
        HttpRange range = HttpRange.parseHeader(rangeHeaderVal, 5);
        assertEquals(3, range.length());
        assertEquals(2, range.start());
        assertEquals(4, range.end());

        rangeHeaderVal = "bytes=-100";
        range = HttpRange.parseHeader(rangeHeaderVal, 5);
        assertEquals(5, range.length());
        assertEquals(0, range.start());
        assertEquals(4, range.end());

        rangeHeaderVal = "bytes=-100";
        range = HttpRange.parseHeader(rangeHeaderVal, 200);
        assertEquals(100, range.length());
        assertEquals(100, range.start());
        assertEquals(199, range.end());

        range = HttpRange.parseHeader(null, 200);
        assertEquals(200, range.length());
        assertEquals(0, range.start());
        assertEquals(199, range.end());
    }

    @Test
    public void testValidFullRange()
    {
        final String rangeHeaderVal = "bytes=0-100";
        final HttpRange range = HttpRange.parseHeader(rangeHeaderVal, 500);
        assertEquals(101, range.length());
        assertEquals(0, range.start());
        assertEquals(100, range.end());
    }

    @Test
    public void testInvalidRangeFormat()
    {
        String rangeHeader = "bytes=2344--3432";
        IllegalArgumentException thrownException = assertThrows(IllegalArgumentException.class, () ->
        {
            HttpRange.parseHeader(rangeHeader, Long.MAX_VALUE);
        });
        String msg = "Invalid range header: bytes=2344--3432. Supported Range formats are bytes=<start>-<end>, " +
                     "bytes=<start>-, bytes=-<suffix-length>";
        assertEquals(msg, thrownException.getMessage());

        thrownException = assertThrows(IllegalArgumentException.class, () ->
        {
            HttpRange.parseHeader("bytes=-", Long.MAX_VALUE);
        });
        msg = "Invalid range header: bytes=-. Supported Range formats are bytes=<start>-<end>, " +
              "bytes=<start>-, bytes=-<suffix-length>";
        assertEquals(msg, thrownException.getMessage());
    }

    @Test
    public void testInvalidSuffixLength()
    {
        final String rangeHeader = "bytes=-0";
        IllegalArgumentException thrownException = assertThrows(IllegalArgumentException.class, () ->
        {
            HttpRange.parseHeader(rangeHeader, Long.MAX_VALUE);
        });
        String msg = "Range does not satisfy boundary requirements";
        assertEquals(msg, thrownException.getMessage());
    }

    @Test
    public void testInvalidRangeBoundary()
    {
        final String rangeHeader = "bytes=9-2";
        IllegalArgumentException thrownException = assertThrows(IllegalArgumentException.class, () ->
        {
            HttpRange.parseHeader(rangeHeader, Long.MAX_VALUE);
        });
        String msg = "Range does not satisfy boundary requirements";
        assertEquals(msg, thrownException.getMessage());
    }

    @Test
    public void testWrongRangeUnitUsed()
    {
        final String rangeVal = "bits=0-";
        IllegalArgumentException thrownException = assertThrows(IllegalArgumentException.class, () ->
        {
            HttpRange.parseHeader(rangeVal, 5);
        });
        String msg = "Invalid range header: bits=0-. Supported Range formats are bytes=<start>-<end>, " +
                     "bytes=<start>-, bytes=-<suffix-length>";
        assertEquals(msg, thrownException.getMessage());
    }

    @Test
    public void testToString()
    {
        final String rangeHeaderVal = "bytes=0-100";
        final HttpRange range = HttpRange.parseHeader(rangeHeaderVal, Long.MAX_VALUE);
        assertEquals("bytes=0-100", range.toString());
    }

    @Test
    public void testInvalidRangeBoundValueInHeader()
    {
        // the right end of range is larger than long
        final String rangeHeader = "bytes=0-1" + Long.MAX_VALUE;
        IllegalArgumentException thrownException = assertThrows(IllegalArgumentException.class, () ->
        {
            HttpRange.parseHeader(rangeHeader, Long.MAX_VALUE);
        });
        String msg = "Invalid range header: bytes=0-19223372036854775807. Supported Range formats are " +
                     "bytes=<start>-<end>, bytes=<start>-, bytes=-<suffix-length>";
        assertEquals(msg, thrownException.getMessage());
    }

    @Test
    public void testIntersect()
    {
        HttpRange range1, range2, expected;
        range1 = HttpRange.of(5, 10);
        range2 = HttpRange.of(9, 15);
        expected = HttpRange.of(9, 10);
        assertEquals(expected, range1.intersect(range2));
        assertEquals(expected, range2.intersect(range1));

        range1 = HttpRange.of(1, 5);
        range2 = HttpRange.of(5, 15);
        expected = HttpRange.of(5, 5);
        assertEquals(expected, range1.intersect(range2));
        assertEquals(expected, range2.intersect(range1));

        range1 = HttpRange.of(1, 15);
        range2 = HttpRange.of(5, 10);
        expected = HttpRange.of(5, 10);
        assertEquals(expected, range1.intersect(range2));
        assertEquals(expected, range2.intersect(range1));
    }

    @Test
    public void testRangesDoNotIntersect()
    {
        HttpRange range1 = HttpRange.of(1, 5);
        HttpRange range2 = HttpRange.of(9, 15);

        assertThrows(RangeException.class, () -> range1.intersect(range2));
    }
}
