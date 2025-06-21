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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.utils.Preconditions;
import org.apache.cassandra.sidecar.common.utils.StringUtils;

/**
 * Request payload for start/stop operations (gossip, native transport, etc.).
 *
 * <p>Valid JSON:</p>
 * <pre>
 *   { "state": "start" }
 *   { "state": "stop"  }
 * </pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class NodeCommandRequestPayload
{
    /**
     * Node Command State
     */
    public enum State
    {
        @JsonProperty("start")
        START,

        @JsonProperty("stop")
        STOP;

        @JsonCreator
        public static State fromString(String s)
        {
            if (s == null)
                throw new IllegalArgumentException("Null state");

            switch (s.trim().toLowerCase())
            {
                case "start":
                    return START;
                case "stop":
                    return STOP;
                default:
                    throw new IllegalArgumentException("Unknown state: " + s);
            }
        }

        @JsonProperty
        public String toValue()
        {
            return name().toLowerCase();
        }
    }

    private final State state;

    /**
     * @param state the desired operation, must be "start" or "stop"
     */
    @JsonCreator
    public NodeCommandRequestPayload(
    @JsonProperty(value = "state", required = true) String state
    )
    {
        Preconditions.checkArgument(StringUtils.isNotEmpty(state),
                                    "state must be provided and non-empty");
        this.state = State.fromString(state);
    }

    /**
     * @return the parsed enum (START or STOP)
     */
    @JsonProperty("state")
    public State state()
    {
        return state;
    }

    @Override
    public String toString()
    {
        return "NodeCommandRequestPayload{state=" + state + '}';
    }
}
