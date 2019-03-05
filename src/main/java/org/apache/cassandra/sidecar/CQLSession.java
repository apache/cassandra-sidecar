package org.apache.cassandra.sidecar;

import java.net.InetSocketAddress;
import java.util.Collections;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.NettyOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
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
    private final InetSocketAddress inet;
    private final WhiteListPolicy wlp;
    private NettyOptions nettyOptions;
    private QueryOptions queryOptions;
    private ReconnectionPolicy reconnectionPolicy;

    @Inject
    public CQLSession(Configuration configuration)
    {
        inet = InetSocketAddress.createUnresolved(configuration.getCassandraHost(), configuration.getCassandraPort());
        wlp = new WhiteListPolicy(new RoundRobinPolicy(), Collections.singletonList(inet));
        this.nettyOptions = new NettyOptions();
        this.queryOptions = new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE);
        this.reconnectionPolicy = new ExponentialReconnectionPolicy(1000,
                                                                    configuration.getHealthCheckFrequencyMillis());
    }

    @VisibleForTesting
    CQLSession(InetSocketAddress target, NettyOptions options)
    {
        inet = target;
        wlp = new WhiteListPolicy(new RoundRobinPolicy(), Collections.singletonList(inet));
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
                cluster = Cluster.builder()
                                 .addContactPointsWithPorts(inet)
                                 .withLoadBalancingPolicy(wlp)
                                 .withQueryOptions(queryOptions)
                                 .withReconnectionPolicy(reconnectionPolicy)
                                 .withoutMetrics()
                                 // tests can create a lot of these Cluster objects, to avoid creating HWTs and
                                 // event thread pools for each we have the override
                                 .withNettyOptions(nettyOptions)
                                 .build();
                localSession = cluster.connect();
            }
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
