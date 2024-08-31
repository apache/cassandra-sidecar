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

package org.apache.cassandra.sidecar.restore;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.common.utils.HttpRange;
import org.assertj.core.util.Lists;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HttpRangesIteratorTest
{
    @Test
    void testIteration()
    {
        HttpRangesIterator iterator = new HttpRangesIterator(19, 10);
        List<HttpRange> ranges = Lists.newArrayList(iterator);
        assertThat(ranges).hasSize(2);
        assertThat(ranges.get(0)).hasToString("bytes=0-9");
        assertThat(ranges.get(1)).hasToString("bytes=10-18");
    }

    @Test
    void testSingleRange()
    {
        HttpRangesIterator iterator = new HttpRangesIterator(19, 20);
        List<HttpRange> ranges = Lists.newArrayList(iterator);
        assertThat(ranges).hasSize(1);
        assertThat(ranges.get(0)).hasToString("bytes=0-18");
    }

    @Test
    void testInvalidArguments()
    {
        assertThatThrownBy(() -> new HttpRangesIterator(0, 1))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("totalBytes must be positive");

        assertThatThrownBy(() -> new HttpRangesIterator(1, 0))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("rangeSize must be positive");
    }
}
