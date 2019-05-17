package org.apache.cassandra.sidecar;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.util.Collections;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Represents a connection to Cassandra cluster. Currently supports returning the local connection only as
 * defined in the Configuration.
 */
@Singleton
public class CQLSession
{
    private static final Logger logger = LoggerFactory.getLogger(CQLSession.class);
    @Nullable
    private Session localSession;
    private final InetSocketAddress address;
    private final WhiteListPolicy wlp;
    private final NettyOptions nettyOptions;
    private final QueryOptions queryOptions;
    private final ReconnectionPolicy reconnectionPolicy;
    @Nullable
    private String username;
    @Nullable
    private String password;
    @Nullable
    private boolean useSSL;
    @Nullable
    private String trustStorePath;
    @Nullable
    private String trustStorePassword;

    @Inject
    public CQLSession(Configuration config)
    {
        address = InetSocketAddress.createUnresolved(config.getCassandraHost(),
                                                     config.getCassandraPort());
        wlp = new WhiteListPolicy(new RoundRobinPolicy(), Collections.singletonList(address));
        this.nettyOptions = new NettyOptions();
        this.queryOptions = new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE);
        this.reconnectionPolicy = new ExponentialReconnectionPolicy(1000,
                                                                    config.getHealthCheckFrequencyMillis());
        this.username = config.getCassandraUsername();
        this.password = config.getCassandraPassword();
        this.useSSL = config.isCassandraSslEnabled();
        this.trustStorePath = config.getCassandraTrustStorePath();
        this.trustStorePassword = config.getCassandraTrustStorePassword();

    }

    @VisibleForTesting
    CQLSession(InetSocketAddress target, NettyOptions options)
    {
        address = target;
        wlp = new WhiteListPolicy(new RoundRobinPolicy(), Collections.singletonList(address));
        this.nettyOptions = options;
        this.queryOptions = new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE);
        reconnectionPolicy = new ExponentialReconnectionPolicy(100, 1000);
    }

    /**
     * Provides a Session connected only to the local node from configuration. If null it means the the connection was
     * not able to be established. The session still might throw a NoHostAvailableException if the local host goes
     * offline or otherwise unavailable.
     *
     * @return Session
     */
    @Nullable
    public synchronized Session getLocalCql()
    {
        Cluster cluster = null;
        try
        {
            if (localSession == null)
            {
                Cluster.Builder builder = Cluster.builder()
                                                 .addContactPointsWithPorts(address)
                                                 .withLoadBalancingPolicy(wlp)
                                                 .withQueryOptions(queryOptions)
                                                 .withReconnectionPolicy(reconnectionPolicy)
                                                 .withoutMetrics()
                                                 // tests can create a lot of these Cluster objects, to avoid creating
                                                 // HWTs and event thread pools for each we have the override
                                                 .withNettyOptions(nettyOptions);

                if (useSSL)
                {
                    try
                    {
                        if (trustStorePath != null && trustStorePassword != null)
                        {
                            KeyStore ks;
                            if (trustStorePath.toLowerCase().endsWith("p12"))
                                ks = KeyStore.getInstance("PKCS12");
                            else
                                ks = KeyStore.getInstance("JKS");

                            SSLContext sslcontext = SSLContext.getInstance("TLS");
                            try (InputStream trustStore = new FileInputStream(trustStorePath))
                            {
                                ks.load(trustStore, trustStorePassword.toCharArray());
                                String algorithm = TrustManagerFactory.getDefaultAlgorithm();
                                TrustManagerFactory tmf = TrustManagerFactory.getInstance(algorithm);
                                tmf.init(ks);
                                sslcontext.init(null, tmf.getTrustManagers(), null);
                            }
                            RemoteEndpointAwareJdkSSLOptions opts = RemoteEndpointAwareJdkSSLOptions.builder()
                                                                                          .withSSLContext(sslcontext)
                                                                                          .build();
                            builder.withSSL(opts);
                        }
                        else
                        {
                            builder.withSSL();
                        }
                    }
                    catch (Exception e)
                    {
                        // this shouldn't happen with validation in CassandraSidecarDaemon, rethrow and kill the Guice
                        // injection
                        throw new RuntimeException(e);
                    }
                }

                if (!Strings.isNullOrEmpty(username) && password != null)
                    builder.withAuthProvider(new PlainTextAuthProvider(username, password));

                cluster = builder.build();
                localSession = cluster.connect();
            }
        }
        catch (AuthenticationException auth)
        {
            logger.error("Cassandra configuration is incorrect.", auth);
        }
        catch (Exception e)
        {
            logger.debug("Failed to reach Cassandra", e);
            if (cluster != null)
            {
                try
                {
                    cluster.close();
                }
                catch (Exception ex)
                {
                    logger.debug("Failed to close cluster in cleanup", ex);
                }
            }
        }
        return localSession;
    }

    public synchronized void close()
    {
        if (localSession != null)
        {
            localSession.getCluster().close();
            localSession = null;
        }
    }
}
