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

package org.apache.cassandra.sidecar.common.server.cluster.locator;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TokenRangeTest
{
    @Test
    void testEquals()
    {
        TokenRange r1 = new TokenRange(1, 100);
        TokenRange r2 = new TokenRange(1, 100);
        TokenRange r3 = new TokenRange(-10, 10);
        assertThat(r1).isEqualTo(r2);
        assertThat(r3).isNotEqualTo(r1)
                      .isNotEqualTo(r2);
    }

    @Test
    void testFirstToken()
    {
        TokenRange range = new TokenRange(1, 100);
        assertThat(range.firstToken()).isEqualTo(Token.from(2));

        TokenRange emptyRange = new TokenRange(1, 1);
        assertThat(emptyRange.firstToken()).isNull();
    }

    @Test
    void testCreateFromJavaDriverTokenRange()
    {
        com.datastax.driver.core.TokenRange ordinaryRange = mockRange(1L, 100L);
        when(ordinaryRange.isWrappedAround()).thenReturn(false);
        when(ordinaryRange.unwrap()).thenCallRealMethod();
        List<TokenRange> ranges = TokenRange.from(ordinaryRange);
        assertThat(ranges).hasSize(1)
                          .isEqualTo(Collections.singletonList(new TokenRange(1, 100)));
    }

    @Test
    void testCreateFromWraparoundJavaDriverTokenRange()
    {
        com.datastax.driver.core.TokenRange range = mockRange(10L, -10L);
        List<com.datastax.driver.core.TokenRange> unwrapped = Arrays.asList(mockRange(10L, Long.MAX_VALUE),
                                                                            mockRange(Long.MIN_VALUE, -10L));
        when(range.unwrap()).thenReturn(unwrapped);
        List<TokenRange> ranges = TokenRange.from(range);
        assertThat(ranges).hasSize(2)
                          .isEqualTo(Arrays.asList(new TokenRange(10, Long.MAX_VALUE),
                                                   new TokenRange(Long.MIN_VALUE, -10L)));
    }

    @Test
    void testRangeEnclose()
    {
        TokenRange r1 = new TokenRange(3, 5);
        TokenRange r2 = new TokenRange(1, 10);
        TokenRange r3 = new TokenRange(10, 11);
        TokenRange r4 = new TokenRange(4, 11);
        assertThat(r2.encloses(r1)).isTrue();
        assertThat(r4.encloses(r3)).isTrue();
        assertThat(r1.encloses(r2)).isFalse();
        assertThat(r3.encloses(r1)).isFalse();
        assertThat(r2.encloses(r3)).isFalse();
        assertThat(r1.encloses(r4)).isFalse();
        assertThat(r4.encloses(r1)).isFalse();
    }

    @Test
    void testOverlaps()
    {
        TokenRange r1 = new TokenRange(3, 5);
        TokenRange r2 = new TokenRange(1, 10);
        TokenRange r3 = new TokenRange(10, 11);
        TokenRange r4 = new TokenRange(4, 11);
        assertThat(r1.overlaps(r2)).isTrue();
        assertThat(r2.overlaps(r1)).isTrue();
        assertThat(r3.overlaps(r4)).isTrue();
        assertThat(r4.overlaps(r3)).isTrue();
        assertThat(r2.overlaps(r4)).isTrue();
        assertThat(r4.overlaps(r2)).isTrue();
        assertThat(r2.overlaps(r3)).isFalse();
        assertThat(r3.overlaps(r2)).isFalse();
    }

    @Test
    void testLargerThan()
    {
        TokenRange r1 = new TokenRange(3, 5);
        TokenRange r2 = new TokenRange(1, 10);
        TokenRange r3 = new TokenRange(10, 11);
        assertThat(r1.largerThan(r2)).isFalse();
        assertThat(r2.largerThan(r1)).isFalse();
        assertThat(r3.largerThan(r1)).isTrue();
        assertThat(r1.largerThan(r3)).isFalse();
        assertThat(r3.largerThan(r2)).isTrue();
        assertThat(r2.largerThan(r3)).isFalse();
    }

    @Test
    void testIntersection()
    {
        TokenRange r1 = new TokenRange(3, 5);
        TokenRange r2 = new TokenRange(1, 10);
        TokenRange r3 = new TokenRange(4, 6);
        TokenRange r4 = new TokenRange(10, 11);
        assertThat(r1.intersection(r2)).isEqualTo(r1);
        assertThat(r2.intersection(r1)).isEqualTo(r1);
        assertThat(r1.intersection(r3)).isEqualTo(new TokenRange(4, 5));
        assertThat(r3.intersection(r1)).isEqualTo(new TokenRange(4, 5));
        assertThat(r2.intersection(r4)).isEqualTo(new TokenRange(10, 10)); // empty range
        assertThat(r2.intersection(r4)).isNotEqualTo(new TokenRange(5, 5)); // but not any empty range
    }

    private com.datastax.driver.core.TokenRange mockRange(long start, long end)
    {
        com.datastax.driver.core.TokenRange range = mock(com.datastax.driver.core.TokenRange.class);
        com.datastax.driver.core.Token startToken = mockToken(start);
        when(range.getStart()).thenReturn(startToken);
        com.datastax.driver.core.Token endToken = mockToken(end);
        when(range.getEnd()).thenReturn(endToken);
        return range;
    }

    private com.datastax.driver.core.Token mockToken(long value)
    {
        com.datastax.driver.core.Token token = mock(com.datastax.driver.core.Token.class);
        when(token.getType()).thenReturn(com.datastax.driver.core.DataType.bigint());
        when(token.getValue()).thenReturn(value);
        return token;
    }
}
