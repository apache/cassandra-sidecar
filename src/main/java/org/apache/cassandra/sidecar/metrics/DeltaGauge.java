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

package org.apache.cassandra.sidecar.metrics;

import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;

/**
 * {@link DeltaGauge} is a Gauge that tracks a cumulative value, which can be updated by passing delta values. The
 * cumulative value tracked is reset once the gauge is read.
 */
public class DeltaGauge implements Gauge<Long>, Metric
{
    private AtomicLong count;

    public DeltaGauge()
    {
        this.count = new AtomicLong();
    }

    /**
     * Updates cumulative value tracked with delta.
     *
     * @param delta value for updating cumulative count.
     */
    public void update(long delta)
    {
        count.addAndGet(delta);
    }

    /**
     * Returns the cumulative value tracked by this gauge and resets it to 0.
     * Note: {@code count} is not expected to overflow
     *
     * @return count value
     */
    @Override
    public Long getValue()
    {
        return count.getAndSet(0);
    }
}
