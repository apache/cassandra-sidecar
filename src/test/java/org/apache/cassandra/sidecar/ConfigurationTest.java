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

package org.apache.cassandra.sidecar;

import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.commons.configuration2.YAMLConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.common.CassandraVersionProvider;

import static org.junit.Assert.assertEquals;

/**
 * Test changes related to sidecar.yaml file.
 */
public class ConfigurationTest
{
    private CassandraVersionProvider versionProvider;

    @BeforeEach
    void setUp()
    {
        Injector injector = Guice.createInjector(Modules.override(new MainModule()).with(new TestModule()));
        versionProvider = injector.getInstance(CassandraVersionProvider.class);
    }

    @Test
    public void testOldSidecarYAMLFormatWithSingleInstance() throws Exception
    {
        MainModule mainModule = new MainModule();
        YAMLConfiguration yamlConf = new YAMLConfiguration();
        try (InputStream stream = new FileInputStream("src/test/resources/sidecar_single_instance.yaml"))
        {
            yamlConf.read(stream);
            InstancesConfig instancesConfig = mainModule.readInstancesConfig(yamlConf, versionProvider);
            assertEquals(1, instancesConfig.instances().size());
            assertEquals("localhost", instancesConfig.instances().get(0).host());
            assertEquals(9042, instancesConfig.instances().get(0).port());
        }
    }

    @Test
    public void testReadingSingleInstanceSectionOverMultipleInstances() throws Exception
    {
        MainModule mainModule = new MainModule();
        YAMLConfiguration yamlConf = new YAMLConfiguration();
        try (InputStream stream = new FileInputStream("src/test/resources/sidecar_with_single_multiple_instances.yaml"))
        {
            yamlConf.read(stream);
            InstancesConfig instancesConfig = mainModule.readInstancesConfig(yamlConf, versionProvider);
            assertEquals(1, instancesConfig.instances().size());
            assertEquals("localhost", instancesConfig.instances().get(0).host());
            assertEquals(9042, instancesConfig.instances().get(0).port());
        }
    }

    @Test
    public void testReadingMultipleInstances() throws Exception
    {
        MainModule mainModule = new MainModule();
        YAMLConfiguration yamlConf = new YAMLConfiguration();
        try (InputStream stream = new FileInputStream("src/test/resources/sidecar_multiple_instances.yaml"))
        {
            yamlConf.read(stream);
            InstancesConfig instancesConfig = mainModule.readInstancesConfig(yamlConf, versionProvider);
            assertEquals(2, instancesConfig.instances().size());
        }
    }
}
