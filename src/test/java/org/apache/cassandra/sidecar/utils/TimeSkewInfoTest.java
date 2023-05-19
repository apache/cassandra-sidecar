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

package org.apache.cassandra.sidecar.utils;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.Configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test {@link TimeSkewInfo}
 */
public class TimeSkewInfoTest
{
    @Test
    public void returnsCurrentTime()
    {
        long currentTime = 12345L;
        TimeProvider timeProvider = () -> currentTime;
        Configuration config = mock(Configuration.class);
        TimeSkewInfo info = new TimeSkewInfo(timeProvider, config);
        assertThat(info.timeSkewResponse().currentTime).isEqualTo(currentTime);
    }

    @Test
    public void returnsMaxSkewInMinutes()
    {
        Configuration config = mock(Configuration.class);
        when(config.allowableSkewInMinutes()).thenReturn(60);
        TimeSkewInfo info = new TimeSkewInfo(TimeProvider.DEFAULT_TIME_PROVIDER, config);
        assertThat(info.timeSkewResponse().allowableSkewInMinutes).isEqualTo(60);
    }
}
