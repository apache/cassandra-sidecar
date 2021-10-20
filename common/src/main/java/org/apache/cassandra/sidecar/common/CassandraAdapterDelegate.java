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

package org.apache.cassandra.sidecar.common;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;


/**
 * Since it's possible for the version of Cassandra to change under us, we need this delegate to wrap the functionality
 * of the underlying Cassandra adapter.  If a server reboots, we can swap out the right Adapter when the driver
 * reconnects.
 *
 * This delegate *MUST* checkSession() before every call, because:
 *
 * 1. The session lazily connects
 * 2. We might need to swap out the adapter if the version has changed
 *
 */
public class CassandraAdapterDelegate implements ICassandraAdapter, Host.StateListener
{
    private final CQLSession cqlSession;
    private final CassandraVersionProvider versionProvider;
    private Session session;
    private SimpleCassandraVersion currentVersion;
    private ICassandraAdapter adapter;
    private Boolean isUp = false;
    private final int refreshRate;

    private static final Logger logger = LoggerFactory.getLogger(CassandraAdapterDelegate.class);
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private boolean registered = false;
    private boolean delegateStarted = false;

    public CassandraAdapterDelegate(CassandraVersionProvider provider, CQLSession cqlSession)
    {
        this(provider, cqlSession, 5000);
    }

    public CassandraAdapterDelegate(CassandraVersionProvider provider, CQLSession cqlSession, int refreshRate)
    {
        this.cqlSession = cqlSession;
        this.versionProvider = provider;
        this.refreshRate = refreshRate;
    }

    public synchronized void start()
    {
        logger.info("Starting health check");
        delegateStarted = true;
        executor.scheduleWithFixedDelay(this::healthCheck, 0, refreshRate, TimeUnit.MILLISECONDS);
        maybeRegisterHostListener();
    }

    private synchronized void maybeRegisterHostListener()
    {
        if (!registered)
        {
            checkSession();
            if (session != null)
            {
                session.getCluster().register(this);
                registered = true;
            }
        }
    }

    public synchronized void stop()
    {
        logger.info("Stopping health check");
        executor.shutdown();
    }

    /**
     * Need to be called before routing the request to the adapter
     * We might end up swapping the adapter out because of a server upgrade
     */
    public synchronized void checkSession()
    {
        if (session == null)
        {
            session = cqlSession.getLocalCql();
            // Without this check there is a cyclic dependency between these methods,
            // start->maybeRegisterHostListener->checkSession->start
            if (!delegateStarted)
            {
                start();
            }
        }
    }

    /**
     * Should be called on initial connect as well as when a server comes back since it might be from an upgrade
     * synchronized so we don't flood the DB with version requests
     *
     * If the healthcheck determines we've changed versions, it should load the proper adapter
     */
    public synchronized void healthCheck()
    {
        Preconditions.checkNotNull(session, "Session is null");
        try
        {
            String version = session.execute("select release_version from system.local")
                    .one()
                    .getString("release_version");
            isUp = true;
            // this might swap the adapter out
            SimpleCassandraVersion newVersion = SimpleCassandraVersion.create(version);
            if (!newVersion.equals(currentVersion))
            {
                currentVersion = SimpleCassandraVersion.create(version);
                adapter = versionProvider.getCassandra(version).create(cqlSession);
                logger.info("Cassandra version change detected.  New adapter loaded: {}", adapter);
            }
            logger.info("Cassandra version {}");
        }
        catch (NoHostAvailableException e)
        {
            logger.error("Unexpected error connecting to Cassandra instance, ", e);
            isUp = false;
        }
    }


    @Override
    public List<NodeStatus> getStatus()
    {
        checkSession();
        return adapter.getStatus();
    }

    @Override
    public void onAdd(Host host)
    {
        healthCheck();
    }

    @Override
    public void onUp(Host host)
    {
        healthCheck();
        isUp = true;
    }

    @Override
    public void onDown(Host host)
    {
        isUp = false;
    }

    @Override
    public void onRemove(Host host)
    {
        healthCheck();
    }

    @Override
    public void onRegister(Cluster cluster)
    {
    }

    @Override
    public void onUnregister(Cluster cluster)
    {
    }

    public boolean isUp()
    {
        return isUp;
    }

    public SimpleCassandraVersion getVersion()
    {
        healthCheck();
        return currentVersion;
    }
}
