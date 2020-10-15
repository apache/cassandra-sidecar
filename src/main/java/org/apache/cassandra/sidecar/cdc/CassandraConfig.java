package org.apache.cassandra.sidecar.cdc;

import java.io.File;
import java.nio.file.Paths;
import java.util.function.Supplier;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.sidecar.Configuration;

/**
 * Custom Cassandra configurator
 */
@Singleton
public class CassandraConfig implements Supplier<Config>
{
    private Config config;

    @Inject
    public CassandraConfig(Configuration config)
    {
        System.setProperty("cassandra.config", config.getCassandraConfigPath());
        this.config = null;
    }

    public synchronized void init() throws IllegalArgumentException
    {
        this.config = new YamlConfigurationLoader().loadConfig();
        // TODO DatabaseDescriptor.initBasicConfigs(); ?
        DatabaseDescriptor.toolInitialization();
        if (!DatabaseDescriptor.isCDCEnabled())
        {
            throw new IllegalArgumentException("CDC is not enabled in Cassandra, CDC reader will not start");
        }

        if (DatabaseDescriptor.getCDCLogLocation() == null || DatabaseDescriptor.getCDCLogLocation().equals(""))
        {
            throw new IllegalArgumentException("cdc_raw_directory location is not set, cannot start the CDC reader");
        }

        File cdcPath = Paths.get(DatabaseDescriptor.getCDCLogLocation()).toFile();

        if (!cdcPath.exists())
        {
            throw new IllegalArgumentException(String.format("Configured Cassandra cdc_raw_directory [%s] doesn't " +
                    "exist", cdcPath));
        }
    }

    public synchronized void muteConfigs()
    {
        assert (this.config != null);
        this.config.hints_directory = null;
        this.config.data_file_directories = new String[0];
        this.config.saved_caches_directory = null;
        Config.setOverrideLoadConfig(this);
        Config.setClientMode(true);
        DatabaseDescriptor.toolInitialization();
    }

    @Override
    public Config get()
    {
        return this.config;
    }
}
