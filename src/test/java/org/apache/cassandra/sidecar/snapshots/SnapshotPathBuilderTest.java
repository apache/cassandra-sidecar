package org.apache.cassandra.sidecar.snapshots;

import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;

@ExtendWith(VertxExtension.class)
class SnapshotPathBuilderTest extends AbstractSnapshotPathBuilderTest
{
    SnapshotPathBuilder initialize(Vertx vertx, InstancesConfig instancesConfig)
    {
        return new SnapshotPathBuilder(vertx.fileSystem(), instancesConfig);
    }
}
