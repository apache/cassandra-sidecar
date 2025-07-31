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
            assertThat(nodeSettings.jmx().partitioner()).isEqualTo("org.apache.cassandra.dht.Murmur3Partitioner");
            assertThat(nodeSettings.jmx().datacenter()).isEqualTo("datacenter1");
            Map<String, String> cqlSettings = new HashMap<>();
            cluster.getFirstRunningInstance()
                   .executeInternalWithResult("SELECT name, value FROM system_views.settings;")
                   .forEach(row -> cqlSettings.put(row.getString("name"), row.getString("value")));
            assertThat(nodeSettings.cassandra()).isEqualTo(cqlSettings);
        });

        cluster.getFirstRunningInstance().nodetool("disablebinary");
        loopAssert(60, () -> {
            HttpResponse<Buffer> responseAfterStop = getBlocking(trustedClient().get(serverWrapper.serverPort, "localhost", "/api/v2/cassandra/settings")
                                                                                .send()
                                                                                .expecting(HttpResponseExpectation.SC_SERVICE_UNAVAILABLE));
            assertThat(responseAfterStop).isNotNull();
            assertThat(responseAfterStop.statusCode()).isEqualTo(SERVICE_UNAVAILABLE.code());
            assertThat(responseAfterStop.bodyAsJsonObject().getString("message")).contains("CQL NodeSettings unavailable");
        });

        cluster.stopUnchecked(cluster.getFirstRunningInstance());
        loopAssert(60, () -> {
            HttpResponse<Buffer> responseAfterStop = getBlocking(trustedClient().get(serverWrapper.serverPort, "localhost", "/api/v2/cassandra/settings")
                                                                                .send()
                                                                                .expecting(HttpResponseExpectation.SC_SERVICE_UNAVAILABLE));
            assertThat(responseAfterStop).isNotNull();
            assertThat(responseAfterStop.statusCode()).isEqualTo(SERVICE_UNAVAILABLE.code());
            assertThat(responseAfterStop.bodyAsJsonObject().getString("message")).contains("NodeSettings unavailable");
        });
    }

    @Override
    protected void initializeSchemaForTest()
    {
        // Do nothing
    }
}
