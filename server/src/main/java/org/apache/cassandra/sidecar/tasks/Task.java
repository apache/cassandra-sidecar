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

import io.vertx.core.Promise;
import org.jetbrains.annotations.Nullable;

/**
 *  * An interface that defines a task that will be executed during the lifecycle of Cassandra Sidecar
 * @param <T>
 */
public interface Task<T>
{
    /**
     * Defines the task body.
     * The method can be considered as executing in a single thread.
     *
     * <br><b>NOTE:</b> the {@code promise} must be completed (as either succeeded or failed) at the end of the run.
     * Failing to do so, the executor will not be able to trigger a new run.
     *
     * @param promise a promise when the execution completes
     */
    void execute(Promise<T> promise);

    /**
     * @return the result of task execution. Null is returned if there is no result
     */
    @Nullable
    T result();

    /**
     * Close any resources it opened.
     * Implementation note: it is encouraged to handle the exceptions during close()
     */
    default void close()
    {
    }

    /**
     * @return descriptive name of the task. It prefers simple class name, if it is non-empty;
     * otherwise, it returns the full class name
     */
    default String name()
    {
        String simpleName = this.getClass().getSimpleName();
        return simpleName.isEmpty() ? this.getClass().getName() : simpleName;
    }
}
