package org.apache.cassandra.sidecar.common.utils;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Utility class for manipulating dates, times, and durations
 */
public final class TimeUtils {
    /**
     * Private constructor that prevents unnecessary instantiation
     *
     * @throws IllegalStateException when called
     */
    private TimeUtils()
    {
        throw new IllegalStateException(getClass() + " is a static utility class and shall not be instantiated");
    }

    /**
     * Returns a random duration with millisecond precision that is uniformly distributed between two provided durations
     *
     * @param minimum minimum possible duration, inclusive
     * @param maximum maximum possible duration, inclusive
     *
     * @return random duration uniformly distributed between two provided durations, with millisecond precision
     *
     * @throws IllegalArgumentException if minimum duration is greater than maximum duration
     */
    public static Duration randomDuration(Duration minimum, Duration maximum)
    {
        Preconditions.checkArgument(minimum.compareTo(maximum) <= 0,
                                    "Minimum duration must be less or equal to maximum duration");
        return Duration.ofMillis(ThreadLocalRandom.current().nextLong(minimum.toMillis(), maximum.toMillis() + 1L));
    }
}
