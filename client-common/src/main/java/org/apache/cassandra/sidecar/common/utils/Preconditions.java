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

package org.apache.cassandra.sidecar.common.utils;

/**
 * A class to prevent introducing a dependency to guava in common and client
 */
public class Preconditions
{
    /**
     * Throws an {@link IllegalArgumentException} when the {@code validCondition} is {@code false}, otherwise
     * no action is taken.
     *
     * @param validCondition the condition to evaluate
     * @param errorMessage   the error message to use for the {@link IllegalArgumentException}
     * @throws IllegalArgumentException when the condition is not valid (i.e. {@code false})
     */
    public static void checkArgument(boolean validCondition, String errorMessage)
    {
        if (!validCondition)
        {
            throw new IllegalArgumentException(errorMessage);
        }
    }
}
