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

package org.apache.cassandra.sidecar.coordination;

import org.apache.cassandra.sidecar.tasks.ExecutionDetermination;

/**
 * Defines an interface to determine whether the current Sidecar instance should execute certain types of
 * operations that need to run on a limited subset of Sidecar instances.
 */
public interface ConditionalExecutor
{
    /**
     * @return a {@link ExecutionDetermination} based on the implementation of the {@link ConditionalExecutor}
     */
    ExecutionDetermination executionDetermination();
}
