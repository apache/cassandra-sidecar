/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.datahub;

import java.nio.charset.Charset;
import java.util.UUID;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An abstract class that has to be extended and instantiated for every Cassandra
 * cluster that needs its schema converted into a DataHub-compliant format
 */
public abstract class IdentifiersProvider
{
    protected static final String URN = "urn";
    protected static final String LI = "li";
    protected static final String DATA_PLATFORM = "dataPlatform";
    protected static final String DATA_PLATFORM_INSTANCE = "dataPlatformInstance";
    protected static final String CONTAINER = "container";
    protected static final String DATASET = "dataset";
    protected static final String PROD = "PROD";  // DataHub requires this to be {@code PROD}

    protected static final ToStringStyle STYLE = new ToStringStyle()
    {{
        setUseShortClassName(false);
        setUseClassName(true);
        setUseIdentityHashCode(false);
        setUseFieldNames(false);
        setContentStart("(");
        setFieldSeparatorAtStart(false);
        setFieldSeparator(",");
        setFieldSeparatorAtEnd(false);
        setContentEnd(")");
        setDefaultFullDetail(true);
        setArrayContentDetail(true);
        setArrayStart("(");
        setArraySeparator(",");
        setArrayEnd(")");
        setNullText("null");
    }};

    /**
     * A public getter method that returns the name of Cassandra Organization
     *
     * @return name of Cassandra organization
     */
    @NotNull
    public String organization()
    {
        return "Cassandra";
    }

    /**
     * A public getter method that returns the name of Cassandra Platform
     *
     * @return name of Cassandra platform
     */
    @NotNull
    public String platform()
    {
        return "cassandra";
    }

    /**
     * A public getter method that returns the name of Cassandra Environment
     *
     * @return name of Cassandra environment
     */
    @NotNull
    public String environment()
    {
        return "ENVIRONMENT";
    }

    /**
     * A public getter method that returns the name of Cassandra Application
     *
     * @return name of Cassandra application
     */
    @NotNull
    public String application()
    {
        return "application";
    }

    /**
     * A public getter method that returns the name of Cassandra Cluster
     *
     * @return name of Cassandra cluster
     */
    @NotNull
    public abstract String cluster();

    /**
     * A public getter method that returns the identifier of Cassandra Cluster
     *
     * @return identifier of Cassandra cluster
     */
    @NotNull
    public UUID identifier()
    {
        return UUID.nameUUIDFromBytes((environment() + MetadataToAspectConverter.DELIMITER
                                     + application() + MetadataToAspectConverter.DELIMITER
                                     + cluster()).getBytes(Charset.defaultCharset()));
    };

    /**
     * A public helper method that prepares the URN of Data Platform
     *
     * @return URN of data platform
     */
    @NotNull
    public String urnDataPlatform()
    {
        return String.format("%s:%s:%s:%s",
                URN,
                LI,
                DATA_PLATFORM,
                platform());
    }

    /**
     * A public helper method that prepares the URN of Data Platform Instance
     *
     * @return URN of data platform instance
     */
    @NotNull
    public String urnDataPlatformInstance()
    {
        return String.format("%s:%s:%s:(%s,%s)",
                URN,
                LI,
                DATA_PLATFORM_INSTANCE,
                urnDataPlatform(),
                identifier());
    }

    /**
     * A public helper method that prepares the URN of Container
     *
     * @param keyspace metadata of keyspace
     * @return URN of container
     */
    @NotNull
    public String urnContainer(@NotNull KeyspaceMetadata keyspace)
    {
        return String.format("%s:%s:%s:%s_%s",
                URN,
                LI,
                CONTAINER,
                identifier(),
                keyspace.getName());
    }

    /**
     * A public helper method that prepares the URN of Dataset
     *
     * @param table metadata of table
     * @return URN of dataset
     */
    @NotNull
    public String urnDataset(@NotNull TableMetadata table)
    {
        return String.format("%s:%s:%s:(%s,%s.%s.%s,%s)",
                URN,
                LI,
                DATASET,
                urnDataPlatform(),
                identifier(),
                table.getKeyspace().getName(),
                table.getName(),
                PROD);
    }

    /**
     * A public method that returns an {@link int} hash code of this object
     *
     * @return hash code of this object
     */
    @Override
    public int hashCode()
    {
        return new HashCodeBuilder()
                .append(this.organization())
                .append(this.platform())
                .append(this.environment())
                .append(this.application())
                .append(this.cluster())
                .append(this.identifier())
                .toHashCode();
    }

    /**
     * A public method that compares this {@link IdentifiersProvider} object to another one
     *
     * @param other another object for comparison
     * @return whether this object is equal to another object
     */
    @Override
    public boolean equals(@Nullable Object other)
    {
        if (other instanceof IdentifiersProvider)
        {
            IdentifiersProvider that = (IdentifiersProvider) other;
            return new EqualsBuilder()
                    .append(this.organization(), that.organization())
                    .append(this.platform(),     that.platform())
                    .append(this.environment(),  that.environment())
                    .append(this.application(),  that.application())
                    .append(this.cluster(),      that.cluster())
                    .append(this.identifier(),   that.identifier())
                    .isEquals();
        }
        else
        {
            return false;
        }
    }

    /**
     * A public method that returns a {@link String} representation of this object
     *
     * @return a {@link String} representation of this object
     **/
    @Override
    @NotNull
    public String toString()
    {
        return new ToStringBuilder(this, STYLE)
                .append(this.organization())
                .append(this.platform())
                .append(this.environment())
                .append(this.application())
                .append(this.cluster())
                .append(this.identifier())
                .toString()
                .replaceAll("\\s", "");

        // Use of a custom {@link ToStringStyle} implementation prevents the hash code from being
        // included into the {@link String} representation; which, in conjunction with the removal
        // of all whitespace characters, simplifies extraction of {@link IdentifierProvider}
        // objects from the execution logs; without negatively affecting the readability much
    }
}
