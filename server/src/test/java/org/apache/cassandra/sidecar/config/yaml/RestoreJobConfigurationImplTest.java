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
import java.util.EnumSet;

import com.fasterxml.jackson.databind.JsonMappingException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.common.data.CredentialType;
import org.apache.cassandra.sidecar.config.RestoreJobConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Tests for the {@code allowed_credential_types} field of the {@code blob_restore} configuration.
 */
class RestoreJobConfigurationImplTest
{
    @DisplayName("Defaults to allowing both STATIC and IAM credential types")
    @Test
    void testDefaultAllowsBothCredentialTypes()
    {
        RestoreJobConfiguration restoreJobConfig = RestoreJobConfigurationImpl.builder().build();
        assertThat(restoreJobConfig.allowedCredentialTypes())
        .containsExactlyInAnyOrder(CredentialType.STATIC, CredentialType.IAM);
    }

    @DisplayName("Parses 'allowed_credential_types' from the 'blob_restore' configuration")
    @Test
    void testParsesAllowedCredentialTypesFromYaml() throws IOException
    {
        String yaml = "blob_restore:\n" +
                "  allowed_credential_types: [IAM]";

        SidecarConfiguration config = SidecarConfigurationImpl.fromYamlString(yaml);
        RestoreJobConfiguration restoreJobConfig = config.restoreJobConfiguration();
        assertThat(restoreJobConfig).isNotNull();
        assertThat(restoreJobConfig.allowedCredentialTypes()).containsExactly(CredentialType.IAM);
    }

    @DisplayName("Parses 'allowed_credential_types' from the 'blob_restore' configuration")
    @Test
    void testParsesInvalidAllowedCredentialTypesFromYaml() throws IOException
    {
        String yaml = "blob_restore:\n" +
                "  allowed_credential_types: []";

        assertThatExceptionOfType(JsonMappingException.class)
                .isThrownBy(() -> SidecarConfigurationImpl.fromYamlString(yaml))
                .withMessageContaining("Invalid allowed_credential_types value: []");

    }

    @DisplayName("Rejects an empty 'allowed_credential_types' set")
    @Test
    void testRejectsEmptyAllowedCredentialTypes()
    {
        assertThatIllegalArgumentException()
        .isThrownBy(() -> RestoreJobConfigurationImpl.builder()
                                                      .allowedCredentialTypes(EnumSet.noneOf(CredentialType.class))
                                                      .build())
        .withMessageContaining("allowedCredentialTypes must not be empty");
    }

    @DisplayName("Rejects an empty 'allowed_credential_types' set once built")
    @Test
    void testRejectsEmptyAllowedCredentialTypes2()
    {
        RestoreJobConfigurationImpl config = RestoreJobConfigurationImpl.builder()
                .build();

        assertThatIllegalArgumentException()
                .isThrownBy(() -> config.setAllowedCredentialTypes(EnumSet.noneOf(CredentialType.class)))
                .withMessageContaining("Invalid allowed_credential_types value");
    }

}
