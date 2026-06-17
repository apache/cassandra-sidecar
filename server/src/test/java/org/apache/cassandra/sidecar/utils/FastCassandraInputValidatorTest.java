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

package org.apache.cassandra.sidecar.utils;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.cassandra.sidecar.config.CassandraInputValidationConfiguration;
import org.apache.cassandra.sidecar.config.ParameterizedClassConfiguration;
import org.apache.cassandra.sidecar.config.yaml.CassandraInputValidationConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.ParameterizedClassConfigurationImpl;
import org.apache.cassandra.sidecar.exceptions.CassandraInputException;

import static org.apache.cassandra.sidecar.config.yaml.CassandraInputValidationConfigurationImpl.DEFAULT_ALLOWED_CHARS_FOR_COMPONENT_NAME;
import static org.apache.cassandra.sidecar.config.yaml.CassandraInputValidationConfigurationImpl.DEFAULT_ALLOWED_CHARS_FOR_NAME;
import static org.apache.cassandra.sidecar.config.yaml.CassandraInputValidationConfigurationImpl.DEFAULT_ALLOWED_CHARS_FOR_QUOTED_NAME;
import static org.apache.cassandra.sidecar.config.yaml.CassandraInputValidationConfigurationImpl.DEFAULT_ALLOWED_CHARS_FOR_RESTRICTED_COMPONENT_NAME;
import static org.apache.cassandra.sidecar.config.yaml.CassandraInputValidationConfigurationImpl.DEFAULT_ALLOWED_CHARS_FOR_SNAPSHOT_NAME;
import static org.apache.cassandra.sidecar.config.yaml.CassandraInputValidationConfigurationImpl.DEFAULT_FORBIDDEN_KEYSPACES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link FastCassandraInputValidator}
 */
class FastCassandraInputValidatorTest extends CassandraInputValidatorTest
{
    @Override
    protected CassandraInputValidator initializeInstanceForTest()
    {
        return new FastCassandraInputValidator();
    }

    @Test
    void testParsingOfConfigurationValues()
    {
        Map<String, String> validTerminationConfig = Map.of("valid_terminations", ".abc,.def",
                                                            "valid_restricted_terminations", ".xml");
        ParameterizedClassConfiguration validatorConfiguration = new ParameterizedClassConfigurationImpl("ignored", validTerminationConfig);
        CassandraInputValidationConfiguration config = new CassandraInputValidationConfigurationImpl(validatorConfiguration,
                                                                                                     DEFAULT_FORBIDDEN_KEYSPACES,
                                                                                                     DEFAULT_ALLOWED_CHARS_FOR_NAME,
                                                                                                     DEFAULT_ALLOWED_CHARS_FOR_QUOTED_NAME,
                                                                                                     DEFAULT_ALLOWED_CHARS_FOR_COMPONENT_NAME,
                                                                                                     DEFAULT_ALLOWED_CHARS_FOR_RESTRICTED_COMPONENT_NAME,
                                                                                                     DEFAULT_ALLOWED_CHARS_FOR_SNAPSHOT_NAME);
        FastCassandraInputValidator validator = new FastCassandraInputValidator(config);
        assertThat(validator.validTerminations).isEqualTo(List.of(".abc", ".def"));
        assertThat(validator.validRestrictedTerminations).isEqualTo(List.of(".xml"));
    }

    @ParameterizedTest(name = "[{0}]")
    @ValueSource(strings = { "", "\"\"",
                             "very_long_table_name_that_exceeds_two_hundred_and_twenty_two_characters_to_test_upper_" +
                             "limit_of_a_table_also_known_as_column_family_the_upper_limit_is_due_to_limitations_by_" +
                             "the_name_of_file_systems_and_it_also_includes_tid_1" })
    void failsWithInvalidLengthForTableName(String tableName)
    {
        assertThatExceptionOfType(CassandraInputException.class)
        .isThrownBy(() -> instance.validateTableName(tableName))
        .withMessageMatching("Invalid length \\d+ for table name: " + tableName);
    }

    @ParameterizedTest(name = "[{0}]")
    @ValueSource(strings = { "", "\"\"", "keyspace_name_can_only_have_48_characters_or_it_will_fail" })
    void failsWithInvalidLengthForKeyspaceName(String keyspace)
    {
        assertThatExceptionOfType(CassandraInputException.class)
        .isThrownBy(() -> instance.validateKeyspaceName(keyspace))
        .withMessageMatching("Invalid length \\d+ for keyspace: " + keyspace);
    }

    @Test
    void failsOnEmptyComponentName()
    {
        assertThatIllegalArgumentException().isThrownBy(() -> instance.validateComponentName(""))
                                            .withMessage("componentName cannot be empty");
    }

    @Test
    void failsOnEmptyRestrictedComponentName()
    {
        assertThatIllegalArgumentException().isThrownBy(() -> instance.validateRestrictedComponentName(""))
                                            .withMessage("componentName cannot be empty");
    }
}
