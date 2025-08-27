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

package org.apache.cassandra.sidecar.common.server.data;

import org.apache.cassandra.sidecar.common.DataObjectBuilder;

/**
 * Data object representing completed compactions rate
 */
public class CompletedCompactionsRateData
{
    public final double meanRate;
    public final double fifteenMinuteRate;

    private CompletedCompactionsRateData(Builder builder)
    {
        this.meanRate = builder.meanRate;
        this.fifteenMinuteRate = builder.fifteenMinuteRate;
    }


    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * {@code CompletedCompactionsRateData} builder static inner class.
     */
    public static final class Builder implements DataObjectBuilder<Builder, CompletedCompactionsRateData>
    {
        private double meanRate;
        private double fifteenMinuteRate;

        private Builder()
        {
        }

        @Override
        public Builder self()
        {
            return this;
        }

        /**
         * Sets the {@code meanRate} and returns a reference to this Builder enabling method chaining.
         *
         * @param meanRate the {@code meanRate} to set
         * @return a reference to this Builder
         */
        public Builder meanRate(double meanRate)
        {
            return update(b -> b.meanRate = meanRate);
        }

        /**
         * Sets the {@code fifteenMinuteRate} and returns a reference to this Builder enabling method chaining.
         *
         * @param fifteenMinuteRate the {@code fifteenMinuteRate} to set
         * @return a reference to this Builder
         */
        public Builder fifteenMinuteRate(double fifteenMinuteRate)
        {
            return update(b -> b.fifteenMinuteRate = fifteenMinuteRate);
        }

        /**
         * Returns a {@code CompletedCompactionsRateData} built from the parameters previously set.
         *
         * @return a {@code CompletedCompactionsRateData} built with parameters of this {@code CompletedCompactionsRateData.Builder}
         */
        @Override
        public CompletedCompactionsRateData build()
        {
            return new CompletedCompactionsRateData(this);
        }
    }
}
