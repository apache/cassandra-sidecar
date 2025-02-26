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

package org.apache.cassandra.sidecar.modules.multibindings;

/**
 * Class keys in the {@link com.google.inject.multibindings.MapBinder} to {@link org.apache.cassandra.sidecar.tasks.PeriodicTask} objects
 */
public interface PeriodicTaskMapKeys
{
    interface ClusterLeaseClaimTaskKey extends ClassKey {}
    interface HealthCheckPeriodicTaskKey extends ClassKey {}
    interface KeyStoreCheckPeriodicTaskKey extends ClassKey {}
    interface ClientKeyStoreCheckPeriodicTaskKey extends ClassKey {}
    interface RestoreJobDiscovererKey extends ClassKey {}
    interface RestoreProcessorKey extends ClassKey {}
    interface RingTopologyRefresherKey extends ClassKey {}
    interface SchemaReportingTaskKey extends ClassKey {}
    interface SidecarPeerHealthMonitorTaskKey extends ClassKey {}
    interface SidecarSchemaInitializerTaskKey extends ClassKey {}
    interface CdcRawDirectorySpaceCleanerTaskKey extends ClassKey {}
}
