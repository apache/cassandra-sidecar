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
