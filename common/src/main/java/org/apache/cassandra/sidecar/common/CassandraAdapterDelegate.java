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
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.jetbrains.annotations.NotNull;


/**
 * Since it's possible for the version of Cassandra to change under us, we need this delegate to wrap the functionality
 * of the underlying Cassandra adapter.  If a server reboots, we can swap out the right Adapter when the driver
 * reconnects.
 *
 * <p>This delegate <b>MUST</b> invoke {@link #checkSession()} before every call, because:</p>
 *
 * <ol>
 * <li>The session lazily connects</li>
 * <li>We might need to swap out the adapter if the version has changed</li>
 * </ol>
 */
public class CassandraAdapterDelegate implements ICassandraAdapter, Host.StateListener
{
    private final CQLSession cqlSession;
    private final CassandraVersionProvider versionProvider;
    private Session session;
    private SimpleCassandraVersion currentVersion;
    private ICassandraAdapter adapter;
    private volatile boolean isUp = false;

    private static final Logger logger = LoggerFactory.getLogger(CassandraAdapterDelegate.class);
    private boolean registered = false;
    private final AtomicBoolean isHealthCheckActive = new AtomicBoolean(false);

    public CassandraAdapterDelegate(CassandraVersionProvider provider, CQLSession cqlSession)
    {
        this.cqlSession = cqlSession;
        this.versionProvider = provider;
    }

    private synchronized void maybeRegisterHostListener(@NotNull Session session)
    {
        if (!registered)
        {
            session.getCluster().register(this);
            registered = true;
        }
    }

    private synchronized void maybeUnregisterHostListener(@NotNull Session session)
    {
        if (registered)
        {
            session.getCluster().unregister(this);
            registered = false;
        }
    }

    /**
     * Make an attempt to obtain the session object.
     *
     * <p>It needs to be called before routing the request to the adapter
     * We might end up swapping the adapter out because of a server upgrade</p>
     */
    public void checkSession()
    {
        if (session != null)
        {
            return;
        }

        synchronized (this)
        {
            if (session == null)
            {
                session = cqlSession.getLocalCql();
            }
        }
    }

    /**
     * Should be called on initial connect as well as when a server comes back since it might be from an upgrade
     * synchronized so we don't flood the DB with version requests
     *
     * <p>If the healthcheck determines we've changed versions, it should load the proper adapter</p>
     */
    public void healthCheck()
    {
        if (isHealthCheckActive.compareAndSet(false, true))
        {
            try
            {
                healthCheckInternal();
            }
            finally
            {
                isHealthCheckActive.set(false);
            }
        }
        else
        {
            logger.debug("Skipping health check because there's an active check at the moment");
        }
    }

    private void healthCheckInternal()
    {
        checkSession();

        Session activeSession = session;
        if (activeSession == null)
        {
            logger.info("No local CQL session is available. Cassandra is down presumably.");
            isUp = false;
            return;
        }

        maybeRegisterHostListener(activeSession);

        try
        {
            String version = activeSession.execute("select release_version from system.local")
                                          .one()
                                          .getString("release_version");
            isUp = true;
            // this might swap the adapter out
            SimpleCassandraVersion newVersion = SimpleCassandraVersion.create(version);
            if (!newVersion.equals(currentVersion))
            {
                currentVersion = newVersion;
                adapter = versionProvider.getCassandra(version).create(cqlSession);
                logger.info("Cassandra version change detected. New adapter loaded: {}", adapter);
            }
            logger.debug("Cassandra version {}", version);
        }
        catch (NoHostAvailableException e)
        {
            logger.error("Unexpected error connecting to Cassandra instance.", e);
            // The cassandra node is down.
            // Unregister the host listener and nullify the session in order to get a new object.
            isUp = false;
            maybeUnregisterHostListener(activeSession);
            session = null;
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
