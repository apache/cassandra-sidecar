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

package org.apache.cassandra.sidecar.config;

import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;

/**
 * Configuration relevant for {@link org.apache.cassandra.sidecar.tasks.PeriodicTask}s
 */
public interface PeriodicTaskConfiguration
{
    /**
     * @return {@code true} if the task is to be executed, {@code false} if the task is to be skipped
     */
    boolean enabled();

    /**
     * @return the initial delay for the first execution of this task after being scheduled or rescheduled
     */
    MillisecondBoundConfiguration initialDelay();

    /**
     * @return how often this task will execute after the previous task has completed the {@link io.vertx.core.Promise}
     * of the execution
     */
    MillisecondBoundConfiguration executeInterval();
}
