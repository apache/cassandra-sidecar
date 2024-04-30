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

package org.apache.cassandra.sidecar.common.server.data;

import java.util.Objects;

/**
 * Represents the name of keyspaces and tables defined by the grammar in
 * <a href="https://cassandra.apache.org/doc/4.1/cassandra/cql/ddl.html#common-definitions">Cassandra CQL common
 * definitions</a>
 */
public class Name
{
    private final String unquotedName;
    private final String maybeQuotedName;

    /**
     * Constructs a {@link Name} object with the provided {@code name}.
     *
     * @param name the name
     */
    public Name(String name)
    {
        this(name, name);
    }

    /**
     * Constructs a {@link Name} object with the provided {@code unquotedName} and {@code maybeQuotedName}
     * parameters.
     *
     * @param unquotedName    the unquoted name
     * @param maybeQuotedName the maybe quoted name
     */
    public Name(String unquotedName, String maybeQuotedName)
    {
        this.unquotedName = Objects.requireNonNull(unquotedName, "the unquoted name is required");
        this.maybeQuotedName = Objects.requireNonNull(maybeQuotedName, "the _maybe_ quoted name is required");
    }

    /**
     * @return the unquoted name
     */
    public String name()
    {
        return unquotedName;
    }

    /**
     * @return the quoted name, if the original input was quoted and if the unquoted name needs to be quoted,
     * or the unquoted name otherwise
     */
    public String maybeQuotedName()
    {
        return maybeQuotedName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Name name = (Name) o;
        return Objects.equals(unquotedName, name.unquotedName)
               && Objects.equals(maybeQuotedName, name.maybeQuotedName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(unquotedName, maybeQuotedName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "Name{" +
               "unquotedName='" + unquotedName + '\'' +
               ", maybeQuotedName='" + maybeQuotedName + '\'' +
               '}';
    }
}
