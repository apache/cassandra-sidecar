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

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Request payload for a repair job
 */
@JsonDeserialize(builder = RepairPayload.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RepairPayload
{
    private static final String TABLES = "tables";
    private static final String IS_PRIMARY_RANGE = "primaryRange";
    private static final String DATACENTER = "datacenter";
    private static final String HOSTS = "hosts";
    private static final String START_TOKEN = "startToken";
    private static final String END_TOKEN = "endToken";
    private static final String REPAIR_TYPE = "repairType";
    private static final String FORCE = "force";
    private static final String VALIDATE = "validate";

    private final List<String> tables;
    private final Boolean isPrimaryRange;
    private final String datacenter;
    private final List<String> hosts;
    private final String startToken;
    private final String endToken;
    private final RepairType repairType;
    private final Boolean force;
    private final Boolean validate;
    
    public static RepairPayload.Builder builder()
    {
        return new RepairPayload.Builder();
    }

    /**
     * Constructs a new {@link RepairPayload} from the configured {@link RepairPayload.Builder}.
     *
     * @param builder the builder used to create this object
     */
    protected RepairPayload(RepairPayload.Builder builder)
    {
        tables = builder.tables;
        repairType = builder.repairType;
        isPrimaryRange = builder.isPrimaryRange;
        datacenter = builder.datacenter;
        hosts = builder.hosts;
        startToken = builder.startToken;
        endToken = builder.endToken;
        force = builder.force;
        validate = builder.validate;
    }

    @Nullable @JsonProperty(TABLES)
    public List<String> tables()
    {
        return tables;
    }

    @JsonProperty(IS_PRIMARY_RANGE)
    public Boolean isPrimaryRange()
    {
        return isPrimaryRange;
    }

    @JsonProperty(DATACENTER)
    public String datacenter()
    {
        return datacenter;
    }

    @JsonProperty(HOSTS)
    public List<String> hosts()
    {
        return hosts;
    }

    @JsonProperty(START_TOKEN)
    public String startToken()
    {
        return startToken;
    }

    @JsonProperty(END_TOKEN)
    public String endToken()
    {
        return endToken;
    }

    @JsonProperty(REPAIR_TYPE)
    public RepairType repairType()
    {
        return repairType;
    }

    @Nullable @JsonProperty(FORCE)
    public Boolean force()
    {
        return force;
    }

    @JsonProperty(VALIDATE)
    public Boolean shouldValidate()
    {
        return validate;
    }

    /**
     * Enum representing types of repair supported
     */
    public enum RepairType
    {
        FULL,
        INCREMENTAL;

        @JsonValue
        public String getValue()
        {
            return name().toLowerCase();
        }

        @JsonCreator
        public static RepairType fromValue(@NotNull String text)
        {
            String trimmed = text.trim();
            try
            {
                if (trimmed.isEmpty())
                {
                    throw new IllegalArgumentException("Unexpected value: " + text);
                }

                return valueOf(trimmed.toUpperCase());
            }
            catch (IllegalArgumentException e)
            {
                throw new IllegalArgumentException("Unexpected value: " + text);
            }
        }
    }

    /**
     * Builder implementation to construct a {@code RepairPayload}
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class Builder implements DataObjectBuilder<RepairPayload.Builder, RepairPayload>
    {
        private List<String> tables;
        private Boolean isPrimaryRange;
        private String datacenter;
        private List<String> hosts;
        private String startToken;
        private String endToken;
        private RepairType repairType;
        private Boolean force;
        private Boolean validate;

        private Builder()
        {
        }

        @Override
        public RepairPayload.Builder self()
        {
            return this;
        }

        @JsonProperty(TABLES)
        public RepairPayload.Builder tables(List<String> tables)
        {
            return update(b -> b.tables = tables);
        }

        @JsonProperty(IS_PRIMARY_RANGE)
        public RepairPayload.Builder isPrimaryRange(boolean isPrimaryRange)
        {
            return update(b -> b.isPrimaryRange = isPrimaryRange);
        }

        @JsonProperty(DATACENTER)
        public RepairPayload.Builder datacenter(String datacenter)
        {
            return update(b -> b.datacenter = datacenter);
        }

        @JsonProperty(REPAIR_TYPE)
        public RepairPayload.Builder repairType(RepairType type)
        {
            return update(b -> b.repairType = type);
        }

        @JsonProperty(HOSTS)
        public RepairPayload.Builder hosts(List<String> hosts)
        {
            return update(b -> b.hosts = hosts);
        }

        @JsonProperty(START_TOKEN)
        public RepairPayload.Builder startToken(String startToken)
        {
            return update(b -> b.startToken = startToken);
        }

        @JsonProperty(END_TOKEN)
        public RepairPayload.Builder endToken(String endToken)
        {
            return update(b -> b.endToken = endToken);
        }

        @JsonProperty(FORCE)
        public RepairPayload.Builder force(boolean force)
        {
            return update(b -> b.force = force);
        }

        @JsonProperty(VALIDATE)
        public RepairPayload.Builder shouldValidate(boolean validate)
        {
            return update(b -> b.validate = validate);
        }

        @Override
        public RepairPayload build()
        {
            return new RepairPayload(this);
        }
    }
}
