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

package org.apache.cassandra.sidecar.client;


/**
 * Represents a consumer for a stream, when using streaming for the Sidecar {@link HttpClient}.
 */
public interface StreamConsumer
{
    /**
     * Called when the {@link StreamBuffer} is ready to be consumed. This method can be called multiple times as
     * data arrives and the {@link StreamBuffer} can be consumed.
     *
     * @param buffer the StreamBuffer that wraps the received data
     */
    void onRead(final StreamBuffer buffer);

    /**
     * Called when the stream has completed
     */
    void onComplete();

    /**
     * Called when the stream has failed with the encountered {@link Throwable throwable}.
     *
     * @param throwable throwable
     */
    void onError(final Throwable throwable);
}
