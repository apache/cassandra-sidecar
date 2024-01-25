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

package org.apache.cassandra.sidecar.utils;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * A factory class that returns the {@link ChecksumVerifier} instance.
 */
public class ChecksumVerifierFactory
{
    @VisibleForTesting
    static final ChecksumVerifier FALLBACK_VERIFIER = (options, filePath) ->
                                                               Future.succeededFuture(filePath);
    private final List<ChecksumVerifier> registeredVerifierList;

    /**
     * Constructs a new factory with the provided {@link ChecksumVerifier} list
     *
     * @param list the list of verifiers to use for the factory
     */
    public ChecksumVerifierFactory(List<ChecksumVerifier> list)
    {
        this.registeredVerifierList = Collections.unmodifiableList(Objects.requireNonNull(list,
                                                                                          "list must be provided"));
    }

    /***
     * Returns the first match for a {@link ChecksumVerifier} from the registered list of verifiers. If none of the
     * verifiers matches, a no-op validator is returned.
     *
     * @param headers the request headers used to test whether a {@link ChecksumVerifier} can be used to verify the
     *                request
     * @return the first match for a {@link ChecksumVerifier} from the registered list of verifiers, or a no-op
     * verifier if none match
     */
    public ChecksumVerifier verifier(MultiMap headers)
    {
        for (ChecksumVerifier candidate : registeredVerifierList)
        {
            if (candidate.canVerify(headers))
            {
                return candidate;
            }
        }
        // Fallback to no-op validator
        return FALLBACK_VERIFIER;
    }
}
