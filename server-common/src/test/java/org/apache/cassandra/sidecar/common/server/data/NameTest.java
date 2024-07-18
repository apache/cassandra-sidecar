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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NameTest
{
    private final Name unquoted = new Name("valid_keyspace");
    private final Name quoted = new Name("\"QuotedName\"");
    private final Name anotherUnquoted = new Name("valid_keyspace");

    @Test
    void testGetNameAndQuotedName()
    {
        assertThat(unquoted.name()).isEqualTo("valid_keyspace");
        assertThat(unquoted.maybeQuotedName()).isEqualTo("valid_keyspace");
        assertThat(quoted.name()).isEqualTo("QuotedName");
        assertThat(quoted.maybeQuotedName()).isEqualTo("\"QuotedName\"");
    }

    @Test
    void testToString()
    {
        assertThat(unquoted).hasToString("Name{unquotedName='valid_keyspace', maybeQuotedName='valid_keyspace'}");
        assertThat(quoted).hasToString("Name{unquotedName='QuotedName', maybeQuotedName='\"QuotedName\"'}");
    }

    @Test
    void testEquals()
    {
        assertThat(unquoted).isEqualTo(anotherUnquoted)
                            .isNotSameAs(anotherUnquoted)
                            .isNotEqualTo(quoted);
    }

    @Test
    void testHashCode()
    {
        assertThat(unquoted).hasSameHashCodeAs(anotherUnquoted)
                            .doesNotHaveSameHashCodeAs(quoted);
    }
}
