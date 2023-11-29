package org.apache.cassandra.sidecar.common.utils;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link TimeUtils} class
 */
public class TimeUtilsTest
{
    @Test
    public void testRandomDurationWithMinimumAboveMaximum()
    {
        assertThrows(IllegalArgumentException.class,
                     () -> TimeUtils.randomDuration(Duration.ofSeconds(2L), Duration.ofSeconds(1L)));
    }

    @Test
    public void testRandomDurationWithMinimumEqualToMaximum()
    {
        assertEquals(Duration.ofSeconds(1L),
                     TimeUtils.randomDuration(Duration.ofSeconds(1L), Duration.ofSeconds(1L)));
    }

    @Test
    public void testRandomDurationWithMinimumBelowMaximum()
    {
        for (int test = 0; test < 3600; test++)
        {
            Duration minimum = Duration.ofSeconds(test);
            Duration maximum = Duration.ofSeconds(test + 1);
            Duration random = TimeUtils.randomDuration(minimum, maximum);

            assertTrue(minimum.compareTo(random) <= 0);
            assertTrue(random.compareTo(maximum) <= 0);
        }
    }
}
