package org.apache.cassandra.sidecar.utils;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.SidecarRateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.models.HttpResponse;
import org.apache.cassandra.sidecar.models.Range;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * General handler for serving files
 */
@Singleton
public class FileStreamer
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileStreamer.class);
    private static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("acquirePermit").setDaemon(true).build());
    private static final Duration DELAY = Duration.ofSeconds(5);
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private final SidecarRateLimiter rateLimiter;

    @Inject
    public FileStreamer(SidecarRateLimiter rateLimiter)
    {
        this.rateLimiter = rateLimiter;
    }

    public void stream(final HttpResponse resp, final File file)
    {
        stream(resp, file, new Range(0, file.length() - 1, file.length()));
    }

    public void stream(final HttpResponse resp, final File file, final Range range)
    {
        if (!file.exists() || !file.isFile())
        {
            resp.setNotFoundStatus("File does not exist or it is not a normal file");
            return;
        }
        if (file.length() == 0)
        {
            resp.setBadRequestStatus("File is empty");
            return;
        }
        acquireAndSend(resp, file, range);
    }

    private void acquireAndSend(HttpResponse response, File file, Range range)
    {
        acquireAndSend(response, file, range, Instant.now());
    }

    /**
     * If permit becomes available within a short time, retry immediately
     */
    private void acquireAndSend(HttpResponse response, File file, Range range, Instant startTime)
    {
        while (!rateLimiter.tryAcquire())
        {
            if (checkRetriesExhausted(startTime))
            {
                LOGGER.error("Retries for acquiring permit exhausted!");
                response.setTooManyRequestsStatus();
                return;
            }

            final long microsToWait = rateLimiter.queryEarliestAvailable(0L);
            if (microsToWait <= 0) // immediately retry
            {
                continue;
            }

            if (TimeUnit.MICROSECONDS.toNanos(microsToWait) >= DELAY.getNano())
            {
                response.setRetryAfterHeader(microsToWait);
            }
            else
            {
                retryStreaming(response, file, range, startTime, microsToWait);
            }
            return;
        }
        LOGGER.info("File {} streamed from path {}", file.getName(), file.getAbsolutePath());
        response.sendFile(file, range);
    }

    private boolean checkRetriesExhausted(Instant startTime)
    {
        return startTime.plus(TIMEOUT).isBefore(Instant.now());
    }

    private void retryStreaming(HttpResponse response, File file, Range range, Instant startTime, long microsToSleep)
    {
        SCHEDULER.schedule(() -> acquireAndSend(response, file, range, startTime), microsToSleep, MICROSECONDS);
    }
}
