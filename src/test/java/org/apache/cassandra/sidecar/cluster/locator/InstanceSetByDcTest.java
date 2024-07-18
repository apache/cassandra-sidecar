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

package org.apache.cassandra.sidecar.cluster.locator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InstanceSetByDcTest
{
    @Test
    void testGetInstanceSetOfDc()
    {
        InstanceSetByDc empty = new InstanceSetByDc(new HashMap<>());
        assertThat(empty.get("dc1")).isNotNull().isEmpty();

        Map<String, Set<String>> mapping = new HashMap<>();
        mapping.put("dc1", new HashSet<>(Arrays.asList("i-1", "i-2")));
        InstanceSetByDc nonEmpty = new InstanceSetByDc(mapping);
        assertThat(nonEmpty.get("dc1")).hasSize(2);
        assertThat(nonEmpty.get("dc2")).isNotNull().isEmpty();
    }
}
