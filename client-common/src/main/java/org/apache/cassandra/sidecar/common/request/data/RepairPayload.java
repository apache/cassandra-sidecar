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
    private final Long startToken;
    private final Long endToken;
    private RepairType repairType;
    private final Boolean force;
    private final Boolean validate;

    /**
     * Constructs a new {@link RepairPayload}.
     */
    public RepairPayload()
    {
        this(builder());
    }

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

    @JsonProperty(TABLES)
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
    public Long startToken()
    {
        return startToken;
    }

    @JsonProperty(END_TOKEN)
    public Long endToken()
    {
        return endToken;
    }

    @JsonProperty(REPAIR_TYPE)
    public RepairType repairType()
    {
        return repairType;
    }

    @JsonProperty(FORCE)
    public Boolean force()
    {
        return force;
    }

    /**
     * {@code NodeSettings} builder static inner class.
     */

    @JsonProperty(VALIDATE)
    public Boolean isValidate()
    {
        return validate;
    }

    /**
     * Enum representing types of repair supported
     */
    public enum RepairType
    {
        FULL("full"),
        INCREMENTAL("incremental");

        private final String value;

        RepairType(String value)
        {
            this.value = value;
        }

        @JsonValue
        public String getValue()
        {
            return value;
        }

        @JsonCreator
        public static RepairType fromValue(String text)
        {
            if (text == null || text.trim().isEmpty())
            {
                return null;
            }

            String normalized = text.toLowerCase();
            for (RepairType type : RepairType.values())
            {
                if (type.getValue().equals(normalized))
                {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unexpected value: " + text);
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
        private Long startToken;
        private Long endToken;
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
        public RepairPayload.Builder startToken(long startToken)
        {
            return update(b -> b.startToken = startToken);
        }

        @JsonProperty(END_TOKEN)
        public RepairPayload.Builder endToken(long endToken)
        {
            return update(b -> b.endToken = endToken);
        }

        @JsonProperty(FORCE)
        public RepairPayload.Builder force(boolean force)
        {
            return update(b -> b.force = force);
        }

        @JsonProperty(VALIDATE)
        public RepairPayload.Builder validate(boolean validate)
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
