package org.apache.cassandra.sidecar;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.exceptions.RangeException;
import org.apache.cassandra.sidecar.models.Range;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RangeTest
 */
public class RangeTest
{
    @Test
    public void testValidPartialRange()
    {
        final String rangeHeaderVal = "bytes=2-";
        final Range range = Range.parseHeader(rangeHeaderVal, 5);
        assertEquals(3, range.length());
        assertEquals(2, range.start());
        assertEquals(4, range.end());
        assertTrue(range.isValidHttpRange());
    }

    @Test
    public void testValidFullRange()
    {
        final String rangeHeaderVal = "bytes=0-100";
        final Range range = Range.parseHeader(rangeHeaderVal, 500);
        assertEquals(101, range.length());
        assertEquals(0, range.start());
        assertEquals(100, range.end());
        assertTrue(range.isValidHttpRange());
    }

    @Test
    public void testInvalidRangeFormat()
    {
        final String rangeVal = "2344--3432";
        IllegalArgumentException thrownException = assertThrows(IllegalArgumentException.class, () -> {
            Range.parse(rangeVal);
        });
        String msg = "Supported Range formats are <start>-<end>, <start>-, -<suffix-length>";
        assertEquals(msg, thrownException.getMessage());
    }

    @Test
    public void testInvalidSuffixLength()
    {
        final String rangeVal = "-0";
        IllegalArgumentException thrownException = assertThrows(IllegalArgumentException.class, () -> {
            Range.parse(rangeVal, Long.MAX_VALUE);
        });
        String msg = "Suffix length in -0 cannot be less than or equal to 0";
        assertEquals(msg, thrownException.getMessage());
    }

    @Test
    public void testInvalidRangeBoundary()
    {
        final String rangeVal = "9-2";
        RangeException thrownException = assertThrows(RangeException.class, () -> {
            Range.parse(rangeVal);
        });
        String msg = "Range does not satisfy boundary requirements";
        assertEquals(msg, thrownException.getMessage());
    }

    @Test
    public void testWrongRangeUnitUsed()
    {
        final String rangeVal = "bits=0-";
        UnsupportedOperationException thrownException = assertThrows(UnsupportedOperationException.class, () -> {
            Range.parseHeader(rangeVal, 5);
        });
        String msg = "Unsupported range unit only bytes are allowed";
        assertEquals(msg, thrownException.getMessage());
    }
}
