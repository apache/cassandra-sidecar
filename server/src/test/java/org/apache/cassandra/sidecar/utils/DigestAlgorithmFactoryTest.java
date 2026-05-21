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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.apache.cassandra.sidecar.HelperTestModules.DigestAlgorithmProviderTestModule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Test {@link DigestAlgorithmFactory} behavior.
 */
class DigestAlgorithmFactoryTest
{
    @Test
    void testGetMD5Algorithm()
    {
        Injector injector = getInjector();
        DigestAlgorithmFactory factory = injector.getInstance(DigestAlgorithmFactory.class);
        DigestAlgorithm result = factory.getDigestAlgorithm("MD5", null);

        assertThat(result).isInstanceOf(JdkMd5DigestProvider.JdkMD5DigestAlgorithm.class);
    }

    @Test
    void testGetMD5AlgorithmCaseInsensitive()
    {
        Injector injector = getInjector();
        DigestAlgorithmFactory factory = injector.getInstance(DigestAlgorithmFactory.class);
        DigestAlgorithm result = factory.getDigestAlgorithm("mD5", null);

        assertThat(result).isInstanceOf(JdkMd5DigestProvider.JdkMD5DigestAlgorithm.class);
    }

    @Test
    void testGetXXHash32AlgorithmWithoutSeed()
    {
        Injector injector = getInjector();
        DigestAlgorithmFactory factory = injector.getInstance(DigestAlgorithmFactory.class);
        DigestAlgorithm result = factory.getDigestAlgorithm("XXHash32", null);

        assertThat(result).isInstanceOf(XXHash32Provider.Lz4XXHash32DigestAlgorithm.class);
    }

    @Test
    void testGetXXHash32AlgorithmCaseInsensitive()
    {
        Injector injector = getInjector();
        DigestAlgorithmFactory factory = injector.getInstance(DigestAlgorithmFactory.class);
        DigestAlgorithm result = factory.getDigestAlgorithm("xxhASh32", null);

        assertThat(result).isInstanceOf(XXHash32Provider.Lz4XXHash32DigestAlgorithm.class);
    }

    @Test
    void testGetXXHash32AlgorithmWithSeed()
    {
        Injector injector = getInjector();
        DigestAlgorithmFactory factory = injector.getInstance(DigestAlgorithmFactory.class);
        DigestAlgorithm result = factory.getDigestAlgorithm("XXHash32", 12345);

        assertThat(result).isInstanceOf(XXHash32Provider.Lz4XXHash32DigestAlgorithm.class);

        DigestAlgorithmProvider xxhash32AlgorithmProvider = injector.getInstance(Key.get(DigestAlgorithmProvider.class, Names.named("xxhash32")));
        verify(xxhash32AlgorithmProvider, times(1)).get(12345);
    }

    @Test
    void testUnsupportedAlgorithm()
    {
        Injector injector = getInjector();
        DigestAlgorithmFactory factory = injector.getInstance(DigestAlgorithmFactory.class);

        assertThatThrownBy(() -> factory.getDigestAlgorithm("SHA256", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported digest algorithm SHA256");
    }


    @Test
    void testEmptyAlgorithmName()
    {
        Injector injector = getInjector();
        DigestAlgorithmFactory factory = injector.getInstance(DigestAlgorithmFactory.class);

        assertThatThrownBy(() -> factory.getDigestAlgorithm(null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Digest algorithm name cannot be null or empty");

        assertThatThrownBy(() -> factory.getDigestAlgorithm("", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Digest algorithm name cannot be null or empty");

        assertThatThrownBy(() -> factory.getDigestAlgorithm("   ", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Digest algorithm name cannot be null or empty");
    }

    @Test
    void testValidateDigestAlgorithm()
    {
        // Valid algorithms should not throw (case-insensitive)
        DigestAlgorithmFactory.validateAlgorithmName("MD5");
        DigestAlgorithmFactory.validateAlgorithmName("XXHash32");
        DigestAlgorithmFactory.validateAlgorithmName("md5");
        DigestAlgorithmFactory.validateAlgorithmName("xxhash32");
        DigestAlgorithmFactory.validateAlgorithmName("XXHASH32");

        // Invalid algorithm should throw with descriptive message
        assertThatThrownBy(() -> DigestAlgorithmFactory.validateAlgorithmName("SHA256"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unsupported digest algorithm SHA256");

        // Null algorithm should throw
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> DigestAlgorithmFactory.validateAlgorithmName(null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("cannot be null or empty");

        // Empty string should throw
        assertThatThrownBy(() -> DigestAlgorithmFactory.validateAlgorithmName(""))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("cannot be null or empty");

        // Blank string should throw
        assertThatThrownBy(() -> DigestAlgorithmFactory.validateAlgorithmName("   "))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("cannot be null or empty");
    }

    Injector getInjector()
    {
        return Guice.createInjector(new DigestAlgorithmProviderTestModule());
    }
}
