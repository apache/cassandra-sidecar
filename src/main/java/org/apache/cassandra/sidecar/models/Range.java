package org.apache.cassandra.sidecar.models;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.validation.constraints.NotNull;

import com.google.common.base.Preconditions;

import org.apache.cassandra.sidecar.exceptions.RangeException;

/**
 * Accepted Range formats are start-end, start-, -suffix_length
 * start-end (start = start index of the range, end = end index of the range, both inclusive)
 * start- (start = start index of the range, end = end of file)
 * -suffix-length (Requested length from end of file)
 */
public class Range
{
    private static final Pattern START_END = Pattern.compile("^(\\d+)-(\\d+)$");
    private static final Pattern PARTIAL = Pattern.compile("^((\\d+)-)$|^(-(\\d+))$");
    private static final String RANGE_UNIT = "bytes";
    private final long start;
    private final long end;
    private final long length;

    private Range(final long start, final long end)
    {
        this.start = start;
        this.end = end;
        this.length = length(start, end);
    }

    public Range(final long start, final long end, final long length)
    {
        this.start = start;
        this.end = end;
        this.length = length;
    }

    private long length(final long start, final long end)
    {
        if (start == 0 && end == Long.MAX_VALUE) // avoid overflow (extra byte)
        {
            return Long.MAX_VALUE;
        }
        return end - start + 1;
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

    public boolean isValidHttpRange()
    {
        return start >= 0 && end >= start && length > 0;
    }

    public Range intersect(@NotNull final Range range)
    {
        if (!(start >= range.start() && start <= range.end()) && !(end >= range.start() && end <= range.end()) &&
            !(range.start() >= start && range.start() <= end) && !(range.end() >= start && range.end() <= end))
        {
            throw new RangeException("Range does not overlap");
        }

        return new Range(Math.max(start, range.start()), Math.min(end, range.end()));
    }

    /**
     * Accepted string formats "start-end", both ends of the range required to be parsed
     * Sample accepted formats "1-2", "232-2355"
     */
    private static Range parseAbsolute(@NotNull String range)
    {
        Matcher m = START_END.matcher(range);

        if (!m.matches())
        {
            throw new IllegalArgumentException("Supported Range formats are <start>-<end>, <start>-, -<suffix-length>");
        }

        final long start = Long.parseLong(m.group(1));
        Preconditions.checkArgument(start >= 0, "Range start can not be negative");

        final long end = Long.parseLong(m.group(2));
        if (end < start)
        {
            throw new RangeException("Range does not satisfy boundary requirements");
        }
        return new Range(start, end);
    }

    public static Range parse(@NotNull String range)
    {
        // since file size is not passed, we set it to 0
        return parse(range, 0);
    }

    /**
     * Accepted string formats "1453-3563", "-22344", "5346-"
     * Sample invalid string formats "8-3", "-", "-0", "a-b"
     *
     * @param fileSize - passed in to convert partial range into absolute range
     */
    public static Range parse(@NotNull String range, final long fileSize)
    {
        Matcher m = PARTIAL.matcher(range);
        if (!m.matches())
        {
            return parseAbsolute(range);
        }

        Preconditions.checkArgument(fileSize > 0, "Reference file size invalid");
        if (range.startsWith("-"))
        {
            final long length = Long.parseLong(m.group(4));
            if (length <= 0)
            {
                throw new IllegalArgumentException("Suffix length in " + range + " cannot be less than or equal to 0");
            }
            Preconditions.checkArgument(length <= fileSize, "Suffix length exceeds");
            return new Range(fileSize - length, fileSize - 1, length);
        }

        final long start = Long.parseLong(m.group(2));
        Preconditions.checkArgument(start >= 0, "Range start can not be negative");
        Preconditions.checkArgument(start < fileSize, "Range exceeds");

        return new Range(start, fileSize - 1);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof Range))
        {
            return false;
        }
        Range range = (Range) o;
        return start == range.start &&
                end == range.end &&
                length == range.length;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(start, end, length);
    }

    /**
     * Accepted RangeHeader formats are bytes=start-end, bytes=start-, bytes=-suffix_length
     */
    public static Range parseHeader(final String header, final long fileSize)
    {
        if (header == null)
        {
            return new Range(0, fileSize - 1, fileSize);
        }
        if (!header.startsWith(RANGE_UNIT + "="))
        {
            throw new UnsupportedOperationException("Unsupported range unit only bytes are allowed");
        }
        return Range.parse(header.substring(header.indexOf("=") + 1), fileSize);
    }
}
