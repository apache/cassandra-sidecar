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

package org.apache.cassandra.sidecar.metrics.route;

/**
 * {@link MeteredEndpoints} maintains simplified version of Monitored routes of Sidecar. These simplified routes are
 * used for tagging {@link org.apache.cassandra.sidecar.metrics.MetricName}. Having a separate metric route, provides
 * a consistent way for naming regardless of endpoint version changes and path parameter changes.
 */
public class MeteredEndpoints
{
    public static final String STREAM_SSTABLE_COMPONENT_ROUTE = "/stream/component";
    public static final String SSTABLE_UPLOAD_ROUTE = "/upload";
    public static final String CREATE_RESTORE_ROUTE = "/restore/create";
    public static final String RESTORE_UPDATE_ROUTE = "/restore/update";
    public static final String RESTORE_ABORT_ROUTE = "/restore/abort";
    public static final String RESTORE_SUMMARY_ROUTE = "/restore/summary";
    public static final String CREATE_RESTORE_SLICE_ROUTE = "/restore/create/slice";
}
