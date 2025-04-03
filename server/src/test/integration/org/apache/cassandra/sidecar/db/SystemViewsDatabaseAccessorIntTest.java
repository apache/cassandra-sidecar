package org.apache.cassandra.sidecar.db;

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;

import static org.assertj.core.api.Assertions.assertThat;

class SystemViewsDatabaseAccessorIntTest extends IntegrationTestBase
{
    @CassandraIntegrationTest(buildCluster = false)
    void testReadSettings(ConfigurableCassandraTestContext cassandraTestContext)
    {
        long cdcSizeLimitInMiB = 5;
        cassandraTestContext.configureAndStartCluster(builder -> {
            builder.appendConfig(config -> config.set("cdc_total_space_in_mb", String.valueOf(cdcSizeLimitInMiB)));
        });
        waitForSchemaReady(10, TimeUnit.SECONDS);

        SystemViewsDatabaseAccessor accessor = injector.getInstance(SystemViewsDatabaseAccessor.class);
        Long cdcTotalSpaceSettings = accessor.getCdcTotalSpaceSetting();
        assertThat(cdcTotalSpaceSettings).isNotNull().isEqualTo(cdcSizeLimitInMiB * 1024 * 1024);
    }
}
