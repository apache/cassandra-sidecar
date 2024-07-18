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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.jetbrains.annotations.NotNull;

/**
 * A better descriptive alias for the dc to instance set mapping
 */
public class InstanceSetByDc
{
    @NotNull
    public Map<String, Set<String>> mapping;

    public InstanceSetByDc(String dcName, Set<String> localDcInstances)
    {
        this.mapping = new HashMap<>(1);
        mapping.put(dcName, localDcInstances);
    }

    public InstanceSetByDc(@NotNull Map<String, Set<String>> mapping)
    {
        this.mapping = mapping;
    }

    public int size()
    {
        return mapping.size();
    }

    public Set<String> keySet()
    {
        return mapping.keySet();
    }

    /**
     * @return the set of instances in the given datacenter; returns an empty set of data
     */
    public Set<String> get(String dcName)
    {
        return mapping.getOrDefault(dcName, Collections.emptySet());
    }

    public boolean containsDatacenter(String dcName)
    {
        return mapping.containsKey(dcName);
    }

    public void forEach(BiConsumer<String, Set<String>> consumer)
    {
        mapping.forEach(consumer);
    }
}
