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

package org.apache.cassandra.sidecar.common.data;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Class response for the {@code TokenRangeReplicasRequest}
 */
public class TokenRangeReplicasResponse
{
    private final List<ReplicaInfo> writeReplicas;
    private final List<ReplicaInfo> naturalReplicas;

    /**
     * Constructs token range replicas response object with given params.
     *
     * @param writeReplicas   list of write replicas {@link ReplicaInfo} instances breakdown by token range
     * @param naturalReplicas list of natural replica {@link ReplicaInfo} instances breakdown by token range
     */
    public TokenRangeReplicasResponse(@JsonProperty("writeReplicas") List<ReplicaInfo> writeReplicas,
                                      @JsonProperty("naturalReplicas") List<ReplicaInfo> naturalReplicas)
    {
        this.writeReplicas = writeReplicas;
        this.naturalReplicas = naturalReplicas;
    }

    /**
     * @return returns the {@link ReplicaInfo} instances representing write replicas for each token range
     */
    @JsonProperty("writeReplicas")
    public List<ReplicaInfo> writeReplicas()
    {
        return writeReplicas;
    }

    /**
     * @return returns the {@link ReplicaInfo} instances representing natural replicas for each token range
     */
    @JsonProperty("naturalReplicas")
    public List<ReplicaInfo> naturalReplicas()
    {
        return naturalReplicas;
    }

    /**
     * Class representing replica instances for a token range grouped by datacenter
     */
    public static class ReplicaInfo
    {
        private final String start;
        private final String end;
        private final Map<String, List<String>> replicasByDatacenter;

        public ReplicaInfo(@JsonProperty("start") String start,
                           @JsonProperty("end") String end,
                           @JsonProperty("replicas") Map<String, List<String>> replicasByDc)
        {
            this.start = start;
            this.end = end;
            this.replicasByDatacenter = replicasByDc;
        }

        /**
         * @return the start value of the token range
         */
        @JsonProperty("start")
        public String start()
        {
            return start;
        }

        /**
         * @return the end value of the token range
         */
        @JsonProperty("end")
        public String end()
        {
            return end;
        }

        /**
         * @return mapping of datacenter to a list of replicas that map to the token range
         */
        @JsonProperty("replicas")
        public Map<String, List<String>> replicasByDatacenter()
        {
            return replicasByDatacenter;
        }

        /**
         * {@inheritDoc}
         */
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ReplicaInfo that = (ReplicaInfo) o;
            return start.equals(that.start)
                   && end.equals(that.end)
                   && replicasByDatacenter.equals(that.replicasByDatacenter);
        }

        /**
         * {@inheritDoc}
         */
        public int hashCode()
        {
            return Objects.hash(start, end, replicasByDatacenter);
        }

        /**
         * {@inheritDoc}
         */
        public String toString()
        {
            return "ReplicaInfo{" +
                   "start='" + start + '\'' +
                   ", end='" + end + '\'' +
                   ", replicasByDatacenter=" + replicasByDatacenter +
                   '}';
        }
    }
}
