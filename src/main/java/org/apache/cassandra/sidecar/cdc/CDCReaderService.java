package org.apache.cassandra.sidecar.cdc;

import java.io.InvalidObjectException;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.sidecar.CQLSession;

/**
 * Cassandra's real-time change data capture service.
 */
@Singleton
public class CDCReaderService implements Host.StateListener
{
    private static final Logger logger = LoggerFactory.getLogger(CDCReaderService.class);
    private final CDCIndexWatcher cdcIndexWatcher;
    private final CDCRawDirectoryMonitor cdcRawDirectoryMonitor;
    private final CQLSession session;
    private final CassandraConfig cassandraConfig;

    @Inject
    public CDCReaderService(CDCIndexWatcher cdcIndexWatcher, CDCRawDirectoryMonitor monitor, CQLSession session,
                            CassandraConfig cassandraConfig)
    {
        this.cdcRawDirectoryMonitor = monitor;
        this.cdcIndexWatcher = cdcIndexWatcher;
        this.session = session;
        this.cassandraConfig = cassandraConfig;
    }

    public synchronized void start()
    {
        try
        {
            // Wait until the Cassandra server is UP to load configs and start subsystems. There's no guarantee the
            // config is valid otherwise.
            waitForCassandraServer();
            Cluster cluster = session.getLocalCql().getCluster();
            if (cluster == null)
            {
                throw new InvalidObjectException("Cannot connect to the local Cassandra node");
            }

            // Ensure Cassandra config is valid and remove mutable data paths from the config
            // to ensure CDC reader doesn't accidentally step on Cassandra data.
            this.cassandraConfig.init();
            // TODO : Load metadata from the CQLSession.
            Schema.instance.loadFromDisk(false);
            this.cassandraConfig.muteConfigs();

            for (String keySpace : Schema.instance.getKeyspaces())
            {
                logger.info("Keyspace : {}", keySpace);
                KeyspaceMetadata keyspaceMetadata = Schema.instance.getKeyspaceMetadata(keySpace);
                if (keyspaceMetadata == null)
                {
                    continue;
                }
                for (TableMetadata tableMetadata : keyspaceMetadata.tablesAndViews())
                {
                    logger.info("Table : {}, CDC enabled ? {}", tableMetadata.name,
                            tableMetadata.params.cdc ? "true" : "false");
                }
            }
            // Start monitoring the cdc_raw directory
            this.cdcRawDirectoryMonitor.startMonitoring();
            // Start reading the current commit log.
            this.cdcIndexWatcher.run();

        }
        catch (Exception ex)
        {
            logger.error("Error starting the CDC reader {}", ex);
            this.stop();
            return;
        }
        logger.info("Successfully started the CDC reader");

    }

    public synchronized void stop()
    {
        logger.info("Stopping CDC reader...");
        this.cdcRawDirectoryMonitor.stop();
        this.cdcIndexWatcher.stop();
        logger.info("Successfully stopped the CDC reader");
    }
    @Override
    public void onAdd(Host host)
    {

    }

    @Override
    public void onUp(Host host)
    {

    }

    @Override
    public void onDown(Host host)
    {

    }

    @Override
    public void onRemove(Host host)
    {

    }

    @Override
    public void onRegister(Cluster cluster)
    {

    }

    @Override
    public void onUnregister(Cluster cluster)
    {

    }

    /**
     * Waiting for the Cassandra server.
     * */
    private void waitForCassandraServer() throws InterruptedException
    {
        long retryIntervalMs = 1;
        Cluster cluster = null;

        while (cluster == null)
        {
            if (this.session.getLocalCql() != null)
            {
                cluster = session.getLocalCql().getCluster();
            }
            if (cluster != null)
            {
                break;
            }
            else
            {
                logger.info("Waiting for Cassandra server to start. Retrying after {} milliseconds",
                        retryIntervalMs);
                Thread.sleep(retryIntervalMs);
                retryIntervalMs *= 2;
            }
        }
    }
}
