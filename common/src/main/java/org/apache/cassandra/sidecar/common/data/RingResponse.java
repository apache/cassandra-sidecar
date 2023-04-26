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

package org.apache.cassandra.sidecar.common.data;

import java.math.BigInteger;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Json format for ring response. It also sorts the entries on adding.
 */
public class RingResponse extends PriorityQueue<RingEntry>
{
    // sort by datacenter first and then by token
    private static final Comparator<RingEntry> DEFAULT_COMPARATOR =
              Comparator.<RingEntry, String>comparing(RingEntry::datacenter)
                        .thenComparing(e -> new BigInteger(e.token()));

    // used by json deserialization
    public RingResponse()
    {
        this(4);
    }

    public RingResponse(int capacity)
    {
        super(capacity, DEFAULT_COMPARATOR);
    }
}
