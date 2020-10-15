package org.apache.cassandra.sidecar.metrics.cdc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.inject.Inject;
import com.google.inject.Singleton;


/**
 * Implements monitor interface, uses Netflix's spectator libraries to report metrics.
 */
@Singleton
public class CDCReaderMonitorLogger implements CDCReaderMonitor
{
    private static final Logger logger = LoggerFactory.getLogger(CDCReaderMonitorLogger.class);

    @Inject
    public CDCReaderMonitorLogger()
    {
    }

    @Override
    public void incSentSuccess()
    {
        logger.info("Successfully sent a commit log entry");
    }

    @Override
    public void incSentFailure()
    {
        logger.info("Failed to send a commit log entry");
    }

    @Override
    public void reportCdcRawDirectorySizeInBytes(long dirSize)
    {
        logger.info("Size of the cdc_raw dir is {}", dirSize);
    }
}
