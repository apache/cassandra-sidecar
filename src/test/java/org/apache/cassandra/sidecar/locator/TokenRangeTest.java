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

package org.apache.cassandra.sidecar.locator;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Token;

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
        assertThat(r1).isEqualTo(r1);
        assertThat(r1).isNotEqualTo(r3);
        assertThat(r3).isNotEqualTo(r1);
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
        List<com.datastax.driver.core.TokenRange> unwrapped = Arrays.asList(mockRange(10L, Long.MIN_VALUE),
                                                                            mockRange(Long.MIN_VALUE, -10L));
        when(range.unwrap()).thenReturn(unwrapped);
        List<TokenRange> ranges = TokenRange.from(range);
        assertThat(ranges).hasSize(2)
                          .isEqualTo(Arrays.asList(new TokenRange(10, Long.MIN_VALUE),
                                                   new TokenRange(Long.MIN_VALUE, -10L)));
    }

    private com.datastax.driver.core.TokenRange mockRange(long start, long end)
    {
        com.datastax.driver.core.TokenRange range = mock(com.datastax.driver.core.TokenRange.class);
        Token startToken = mockToken(start);
        when(range.getStart()).thenReturn(startToken);
        Token endToken = mockToken(end);
        when(range.getEnd()).thenReturn(endToken);
        return range;
    }

    private Token mockToken(long value)
    {
        Token token = mock(Token.class);
        when(token.getType()).thenReturn(DataType.bigint());
        when(token.getValue()).thenReturn(value);
        return token;
    }
}
