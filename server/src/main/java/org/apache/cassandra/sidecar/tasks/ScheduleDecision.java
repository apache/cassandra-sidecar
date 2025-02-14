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

package org.apache.cassandra.sidecar.tasks;

/**
 * Determines whether executions can proceed, be skipped, or the {@link PeriodicTask} should be rescheduled
 */
public enum ScheduleDecision
{
    /**
     * Execute the current run and continue to schedule the next run in {@link PeriodicTask#delay()}
     */
    EXECUTE,

    /**
     * Skip the current run and continue to schedule the next run in {@link PeriodicTask#delay()}
     */
    SKIP,

    /**
     * Similar to {@link SKIP}. Skip the current run, but schedule the next run in {@link PeriodicTask#initialDelay()}
     */
    RESCHEDULE;
}
