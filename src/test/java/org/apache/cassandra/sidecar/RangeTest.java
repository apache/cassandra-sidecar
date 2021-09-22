package org.apache.cassandra.sidecar;

import org.junit.Test;

import org.apache.cassandra.sidecar.exceptions.RangeException;
import org.apache.cassandra.sidecar.models.Range;

/**
 * RangeTest
 */
public class RangeTest
{
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidRangeFormat()
    {
        final String rangeVal = "2344--3432";
        Range.parse(rangeVal);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidSuffixLength()
    {
        final String rangeVal = "-0";
        Range.parse(rangeVal, Long.MAX_VALUE);
    }

    @Test(expected = RangeException.class)
    public void testInvalidRangeBoundary()
    {
        final String rangeVal = "9-2";
        Range.parse(rangeVal);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWrongRangeUnitUsed()
    {
        final String rangeVal = "bits=0-";
        Range.parseHeader(rangeVal, 5);
    }
}
