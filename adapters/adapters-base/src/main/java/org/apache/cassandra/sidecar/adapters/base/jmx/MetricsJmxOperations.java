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

package org.apache.cassandra.sidecar.adapters.base.jmx;

/**
 * An interface that pulls methods from the Cassandra Metrics Proxy
 */
public interface MetricsJmxOperations
{
    String METRICS_OBJ_TYPE_KEYSPACE_TABLE_FORMAT = "org.apache.cassandra.metrics:type=Table,keyspace=%s,scope=%s,name=%s";

    /**
     * Retrieves the value of the metric of type {@link com.codahale.metrics.Gauge}
     * @return the value of the Gauge metric
     */
    Object getValue();

    /**
     * Retrieves the value of the metric of type {@link com.codahale.metrics.Counter}
     * @return the value of the Counter metric
     */
    long getCount();
}
