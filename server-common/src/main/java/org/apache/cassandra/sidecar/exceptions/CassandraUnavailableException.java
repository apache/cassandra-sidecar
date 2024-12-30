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

package org.apache.cassandra.sidecar.exceptions;

/**
 * Exception thrown when Cassandra instance service is unavailable to Sidecar
 */
public class CassandraUnavailableException extends RuntimeException
{
    public CassandraUnavailableException(Service service, String additionalMessage)
    {
        super(service.toExceptionMessage() + ". " + additionalMessage);
    }

    public CassandraUnavailableException(Service service, Throwable cause)
    {
        super(service.toExceptionMessage(), cause);
    }

    /**
     * Cassandra service types
     */
    public enum Service
    {
        /**
         * JMX service
         */
        JMX,
        /**
         * CQL/native service
         */
        CQL,
        /**
         * Both CQL and JMX
         */
        CQL_AND_JMX;

        public String toExceptionMessage()
        {
            return "Cassandra " + this.name() + " service is unavailable";
        }
    }
}
