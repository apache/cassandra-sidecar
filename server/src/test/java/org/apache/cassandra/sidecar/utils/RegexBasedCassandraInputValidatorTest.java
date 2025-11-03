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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.cassandra.sidecar.exceptions.CassandraInputException;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for {@link RegexBasedCassandraInputValidator}
 */
class RegexBasedCassandraInputValidatorTest extends CassandraInputValidatorTest
{
    @Override
    protected CassandraInputValidator initializeInstanceForTest()
    {
        return new RegexBasedCassandraInputValidator();
    }

    @ParameterizedTest(name = "[{0}]")
    @ValueSource(strings = { "", "\"\"",
                             "very_long_table_name_that_exceeds_two_hundred_and_twenty_two_characters_to_test_upper_limit" +
                             "_of_a_table_also_known_as_column_family_the_upper_limit_is_due_to_limitations_by_the_name" +
                             "_of_file_systems_and_it_also_includes_tid_1" })
    void failsWithInvalidLengthForTableName(String tableName)
    {
        assertThatExceptionOfType(CassandraInputException.class)
        .isThrownBy(() -> instance.validateTableName(tableName))
        .withMessage("Invalid characters in table name: " + tableName);
    }

    @ParameterizedTest(name = "[{0}]")
    @ValueSource(strings = { "", "\"\"", "keyspace_name_can_only_have_48_characters_or_it_will_fail" })
    void failsWithInvalidLengthForKeyspaceName(String keyspace)
    {
        assertThatExceptionOfType(CassandraInputException.class)
        .isThrownBy(() -> instance.validateKeyspaceName(keyspace))
        .withMessageMatching("Invalid characters in keyspace: " + keyspace);
    }

    @Test
    void failsOnEmptyComponentName()
    {
        assertThatIllegalArgumentException().isThrownBy(() -> instance.validateComponentName(""))
                                            .withMessage("Invalid component name: ");
    }

    @Test
    void failsOnEmptyRestrictedComponentName()
    {
        assertThatIllegalArgumentException().isThrownBy(() -> instance.validateRestrictedComponentName(""))
                                            .withMessage("Invalid component name: ");
    }
}
