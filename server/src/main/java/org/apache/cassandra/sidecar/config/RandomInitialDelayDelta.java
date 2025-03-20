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

package org.apache.cassandra.sidecar.config;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;

/**
 * A randomized delta value that adds jitter to the initial delay configuration of a
 * {@link org.apache.cassandra.sidecar.tasks.PeriodicTask}
 */
public interface RandomInitialDelayDelta
{
    /**
     * @return the configured value for the initial delay
     */
    MillisecondBoundConfiguration initialDelayRandomDelta();

    /**
     * Returns a random delta delay in milliseconds. Internally it uses current's {@link ThreadLocalRandom}
     * to calculate the next long value.
     *
     * @return a random delta delay in milliseconds
     */
    default long randomDeltaDelayMillis()
    {
        long delta = initialDelayRandomDelta().toMillis();
        return delta > 0
               ? ThreadLocalRandom.current().nextLong(delta)
               : 0;
    }
}
