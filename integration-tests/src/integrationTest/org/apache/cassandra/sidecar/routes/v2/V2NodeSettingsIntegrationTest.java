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
package org.apache.cassandra.sidecar.routes.v2;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.vertx.core.VertxException;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpResponseExpectation;
import io.vertx.ext.web.client.HttpResponse;
import org.apache.cassandra.sidecar.common.response.v2.V2NodeSettings;
import org.apache.cassandra.sidecar.testing.SharedClusterSidecarIntegrationTestBase;

import static io.netty.handler.codec.http.HttpResponseStatus.SERVICE_UNAVAILABLE;
import static org.apache.cassandra.testing.utils.AssertionUtils.getBlocking;
import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * V2NodeSettingsIntegrationTest is responsible for verifying the behavior of the /api/v2/cassandra/settings
 * endpoint. This includes:
 *  - Node settings are returned when the node is healthy.
 *  - A specific error is returned when CQL is not healthy.
 *  - A separate error is returned when the node is down.
 */
public class V2NodeSettingsIntegrationTest extends SharedClusterSidecarIntegrationTestBase
{

    @Test
    public void testV2NodeSettings()
    {
        // Start by setting a known configuration for concurrent_reads
        String concurrencyKey = "concurrent_reads";
        String expectedValue = "20";
        cluster.getFirstRunningInstance().nodetool("setconcurrency", "READ", expectedValue);
        ensureSettingsAvailable(concurrencyKey, expectedValue);

        // Disabling NTR should make CQL settings become unavailable
        cluster.getFirstRunningInstance().nodetool("disablebinary");
        ensureSettingsBecomeUnavailable("CQL NodeSettings unavailable");

        // Re-enable NTR, settings should become available again. The value of concurrent_reads should not change.
        cluster.getFirstRunningInstance().nodetool("enablebinary");
        ensureSettingsAvailable(concurrencyKey, expectedValue);

        // Changing a configuration should eventually reflect in settings API.
        expectedValue = "10";
        cluster.getFirstRunningInstance().nodetool("setconcurrency", "READ", expectedValue);
        ensureSettingsAvailable(concurrencyKey, expectedValue);

        cluster.stopUnchecked(cluster.getFirstRunningInstance());
        ensureSettingsBecomeUnavailable("NodeSettings unavailable");
    }

    private void ensureSettingsBecomeUnavailable(String errorMessage)
    {
        loopAssert(60, () -> {
            HttpResponse<Buffer> responseAfterStop = getBlocking(trustedClient().get(serverWrapper.serverPort, "localhost", "/api/v2/cassandra/settings")
                                                                                .send()
                                                                                .expecting(HttpResponseExpectation.SC_SERVICE_UNAVAILABLE));
            assertThat(responseAfterStop).isNotNull();
            assertThat(responseAfterStop.statusCode()).isEqualTo(SERVICE_UNAVAILABLE.code());
            assertThat(responseAfterStop.bodyAsJsonObject().getString("message")).contains(errorMessage);
        });
    }

    private void ensureSettingsAvailable(String expectedSettingKey, String expectedSettingValue)
    {
        loopAssert(60, () -> {
            HttpResponse<Buffer> response = null;
            try
            {
                response = getBlocking(trustedClient().get(serverWrapper.serverPort, "localhost", "/api/v2/cassandra/settings")
                                                      .send()
                                                      .expecting(HttpResponseExpectation.SC_OK));
            }
            catch (VertxException e)
            {
                Assertions.fail(e);
            }
            V2NodeSettings nodeSettings = response.bodyAsJson(V2NodeSettings.class);
            assertThat(nodeSettings).isNotNull();
            Map<String, String> cqlSettings = new HashMap<>();
            cluster.getFirstRunningInstance()
                   .executeInternalWithResult("SELECT name, value FROM system_views.settings;")
                   .forEach(row -> cqlSettings.put(row.getString("name"), row.getString("value")));
            assertThat(nodeSettings.nodeSettings()).isEqualTo(cqlSettings);
            assertThat(cqlSettings.get(expectedSettingKey)).isEqualTo(expectedSettingValue);
        });
    }

    @Override
    protected void initializeSchemaForTest()
    {
        // Do nothing
    }
}
