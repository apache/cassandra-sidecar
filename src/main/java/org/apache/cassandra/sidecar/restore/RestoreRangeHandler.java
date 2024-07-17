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

package org.apache.cassandra.sidecar.restore;

import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.db.RestoreRange;

/**
 * A handler that processes a restore slice
 */
public interface RestoreRangeHandler extends Handler<Promise<RestoreRange>>
{
    /**
     * @return range the handler processes
     */
    RestoreRange range();

    /**
     * @return the elapsed time in nanoseconds if the task has started processing, -1 otherwise
     */
    long elapsedInNanos();
}
