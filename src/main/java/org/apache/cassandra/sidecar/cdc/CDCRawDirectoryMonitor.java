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

package org.apache.cassandra.sidecar.cdc;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Timer;
import java.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.sidecar.metrics.cdc.CDCReaderMonitor;

/**
 * Monitors the cdc_raw directory, cleanup unused commit logs and report metrics
 * */
@Singleton
public class CDCRawDirectoryMonitor extends TimerTask
{

    private final Timer timer;
    private final CDCReaderMonitor monitor;
    private volatile boolean running;
    private static final Logger logger = LoggerFactory.getLogger(CDCRawDirectoryMonitor.class);

    @Inject
    CDCRawDirectoryMonitor(CDCReaderMonitor monitor)
    {
        this.timer = new Timer();
        this.monitor = monitor;
        this.running = false;
    }

    /**
     * Starts the background thread to monitor the cdc_raw dir.
     * */
    public void startMonitoring()
    {
        this.running = true;
        timer.schedule(this, 0, DatabaseDescriptor.getCDCDiskCheckInterval());
    }

    @Override
    public void run()
    {
        if (!this.running)
        {
            return;
        }
        // TODO : Don't be someone who just complains, do some useful work, clean files older than
        //  the last persisted bookmark.
        this.monitor.reportCdcRawDirectorySizeInBytes(getCdcRawDirectorySize());
    }

    public synchronized void stop()
    {
        if (!this.running)
        {
            return;
        }
        this.running = false;
        this.timer.cancel();
    }


    private long getCdcRawDirectorySize()
    {
        long dirSize = 0;
        try (DirectoryStream<Path> stream =
                     Files.newDirectoryStream(Paths.get(DatabaseDescriptor.getCDCLogLocation())))
        {
            for (Path path : stream)
            {
                if (!Files.isDirectory(path))
                {
                    dirSize += Files.size(path);
                }
            }
        }
        catch (IOException ex)
        {
            logger.error("Error when calculating size of the cdc_raw dir {} : {}",
                    DatabaseDescriptor.getCDCLogLocation(), ex);
        }
        return dirSize;
    }
}
