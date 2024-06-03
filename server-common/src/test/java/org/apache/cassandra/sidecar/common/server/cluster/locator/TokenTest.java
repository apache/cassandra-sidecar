/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.common.server.cluster.locator;

import java.math.BigInteger;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TokenTest
{
    @Test
    void testCreateToken()
    {
        Token t1 = Token.from(1);
        Token t2 = Token.from(BigInteger.ONE);
        Token t3 = Token.from("1");
        assertThat(t1).isEqualTo(t2).isEqualTo(t3);
    }

    @Test
    void testIncrement()
    {
        Token t1 = Token.from(1);
        Token t2 = t1.increment();
        assertThat(t2).isEqualTo(Token.from(2));
    }

    @Test
    void testCompare()
    {
        Token t1 = Token.from(1);
        Token t2 = Token.from(2);
        Token t3 = Token.from(1);
        assertThat(t1).isLessThan(t2)
                      .isEqualByComparingTo(t3);
        assertThat(t2).isGreaterThan(t1);
    }
}
