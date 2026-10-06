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

package org.apache.cassandra.sidecar.config.yaml;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.cassandra.sidecar.config.Validatable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class ValidatingDeserializerTest
{
    private final ObjectMapper mapper = newMapper();

    @Test
    void testValidConfiguration() throws IOException
    {
        String yaml = "minimum: 2\n" +
                      "maximum: 9";

        TestConfiguration config = mapper.readValue(yaml, TestConfiguration.class);

        assertThat(config.minimum).isEqualTo(2);
        assertThat(config.maximum).isEqualTo(9);
    }

    @Test
    void testInvalidCrossFieldConfiguration()
    {
        String yaml = "minimum: 8\n" +
                      "maximum: 3";

        assertThatExceptionOfType(JsonMappingException.class)
        .isThrownBy(() -> mapper.readValue(yaml, TestConfiguration.class))
        .withRootCauseInstanceOf(IllegalArgumentException.class)
        .withMessageContaining("Validation failed")
        .withMessageContaining("minimum cannot be greater than maximum");
    }

    @Test
    void testNestedConfigurationIsValidated()
    {
        String yaml = "nested:\n" +
                      "  minimum: 8\n" +
                      "  maximum: 3";

        assertThatExceptionOfType(JsonMappingException.class)
        .isThrownBy(() -> mapper.readValue(yaml, ParentTestConfiguration.class))
        .withRootCauseInstanceOf(IllegalArgumentException.class)
        .withMessageContaining("minimum cannot be greater than maximum");
    }

    @Test
    void testTypesThatDoNotImplementValidatableAreNotValidated() throws IOException
    {
        String yaml = "minimum: 8\n" +
                      "maximum: 3";

        PlainTestConfiguration config = mapper.readValue(yaml, PlainTestConfiguration.class);

        assertThat(config.minimum).isEqualTo(8);
        assertThat(config.maximum).isEqualTo(3);
    }

    private static ObjectMapper newMapper()
    {
        SimpleModule module = new SimpleModule();
        module.setDeserializerModifier(new ValidatingBeanDeserializerModifier());
        return new ObjectMapper(new YAMLFactory()).registerModule(module);
    }

    public static class TestConfiguration implements Validatable
    {
        @JsonProperty("minimum")
        public int minimum = 1;

        @JsonProperty("maximum")
        public int maximum = 10;

        public TestConfiguration()
        {
            validate();
        }

        @Override
        public void validate()
        {
            if (minimum > maximum)
            {
                throw new IllegalArgumentException("minimum cannot be greater than maximum");
            }
        }
    }

    public static class ParentTestConfiguration
    {
        @JsonProperty("nested")
        public TestConfiguration nested;
    }

    public static class PlainTestConfiguration
    {
        @JsonProperty("minimum")
        public int minimum;

        @JsonProperty("maximum")
        public int maximum;
    }
}
