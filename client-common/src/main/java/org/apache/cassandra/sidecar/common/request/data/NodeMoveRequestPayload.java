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

package org.apache.cassandra.sidecar.common.request.data;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request payload for node move operations.
 *
 * <p>Valid JSON:</p>
 * <pre>
 *   { "newToken": "123456789" }
 * </pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class NodeMoveRequestPayload
{
    private final String newToken;

    /**
     * @param newToken the new token for the node to move to
     */
    @JsonCreator
    public NodeMoveRequestPayload(@JsonProperty(value = "newToken", required = true) String newToken)
    {
        this.newToken = Objects.requireNonNull(newToken, "newToken is required");
    }

    /**
     * @return the new token for the node to move to
     */
    @JsonProperty("newToken")
    public String newToken()
    {
        return newToken;
    }

    @Override
    public String toString()
    {
        return "NodeMoveRequestPayload{newToken='" + newToken + "'}";
    }
}
